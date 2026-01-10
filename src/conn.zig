const std = @import("std");
const lib = @import("lib.zig");
const Buffer = @import("buffer").Buffer;

const proto = lib.proto;
const types = lib.types;
const Pool = lib.Pool;
const Stmt = lib.Stmt;
const SSLCtx = lib.SSLCtx;
const Reader = lib.Reader;
const Result = lib.Result;
const Stream = lib.Stream;
const Timeout = lib.Timeout;
const QueryRow = lib.QueryRow;
const has_openssl = lib.has_openssl;

const os = std.os;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const Conn = struct {
    // If we own the ssl context (which only happens if the connection is
    // created directly and NOT through a pool), then we have to free it
    _ssl_ctx: ?*SSLCtx,

    // If we get a postgreSQL error, this will be set.
    err: ?proto.Error,

    // The underlying data for err
    _err_data: ?[]const u8,

    _stream: Stream,

    _pool: ?*Pool = null,

    // The current transation state, this is whatever the last ReadyForQuery
    // message told us
    _state: State,

    // A buffer used for writing to PG. This can grow dynamically as needed.
    _buf: Buffer,

    // Used to read data from PG. Has its own buffer which can grow dynamically
    _reader: Reader,

    _allocator: Allocator,

    // Holds information describing the query that we're executing. If the query
    // returns more columns than an appropriately sized ResultState is created as
    // needed.
    _result_state: Result.State,

    // Holds information describing the parameters that PG is expecting. If the
    // query has more parameters, than an appropriately sized one is created.
    // This is separate from _result_state because:
    //   (a) they are populated separately
    //   (b) have distinct lifetimes
    //   (c) they likely have different lengths;
    _param_oids: []i32,

    // cache_name => data necessary to re-execute previously prepared statement.
    _prepared_statements: std.StringHashMapUnmanaged(Stmt.Describe),

    // Store PID and secret key for cancellation
    _process_id: i32 = 0,
    _secret_key: i32 = 0,

    // Store connection options for cancellation
    _opts: Opts,

    // Optional callback for notices
    _notice_cb: ?*const fn (void, proto.NoticeResponse) void = null,

    const State = enum {
        idle,

        // something bad happened
        fail,

        // we're doing a query
        query,

        // we're in a transaction
        transaction,

        // we're in a copy in state
        copy_in,

        // we're in a copy out state
        copy_out,
    };

    pub const Opts = struct {
        host: ?[]const u8 = null,
        port: ?u16 = null,
        write_buffer: ?u16 = null,
        read_buffer: ?u16 = null,
        result_state_size: u16 = 32,
        tls: TLS = .off,
        _hostz: ?[:0]const u8 = null,

        // Callback for notices
        notice_cb: ?*const fn (void, proto.NoticeResponse) void = null,

        pub const TLS = union(enum) {
            off: void,
            require: void,
            verify_full: ?[]const u8,
        };
    };

    pub const AuthOpts = struct {
        username: []const u8 = "postgres",
        password: ?[]const u8 = null,
        database: ?[]const u8 = null,
        timeout: u32 = 10_000,
        application_name: ?[]const u8 = null,
        startup_parameters: ?std.StringHashMap([]const u8) = null,
    };

    pub const QueryOpts = struct {
        timeout: ?u32 = null,
        column_names: bool = lib.default_column_names,

        allocator: ?Allocator = null,
        // Whether a call to result.deinit() should automatically release the
        // connection back to the pool. Meant to be used internally by pool.query()
        // and the other pool utility wrappers, but applications might find it useful
        // to use in their own helpers
        release_conn: bool = false,

        // When not null, the prepared statement will be cached and re-used
        // by subsequent queries using the same name.
        cache_name: ?[]const u8 = null,
    };

    pub const PipelineSession = struct {
        conn: *Conn,
        allocator: Allocator,

        pub fn query(self: *PipelineSession, sql: []const u8, args: anytype) !void {
            const buf = &self.conn._buf;

            // We use the unnamed statement ("") and unnamed portal ("").
            // We need to infer OIDs for parameters.
            const ArgsType = @TypeOf(args);
            const args_info = @typeInfo(ArgsType);
            if (args_info != .@"struct" or !args_info.@"struct".is_tuple) {
                @compileError("args must be a tuple");
            }

            const fields = args_info.@"struct".fields;
            const param_count = fields.len;

            // 1. Parse ('P')
            {
                // len = 4 + name_len(0+1) + sql_len(N+1) + param_count(2) + param_oids(4*N)
                const payload_len = 4 + 1 + sql.len + 1 + 2 + (param_count * 4);
                try buf.ensureUnusedCapacity(1 + payload_len);
                buf.writeByteAssumeCapacity('P');
                buf.writeIntBigAssumeCapacity(u32, @intCast(payload_len));
                buf.writeByteAssumeCapacity(0); // unnamed statement
                buf.writeAssumeCapacity(sql);
                buf.writeByteAssumeCapacity(0);
                buf.writeIntBigAssumeCapacity(u16, @intCast(param_count));

                inline for (fields) |field| {
                    const oid = types.inferOid(field.type);
                    buf.writeIntBigAssumeCapacity(i32, oid);
                }
            }

            // 2. Bind ('B')
            {
                // Since we don't know the exact length of encoded values yet, we rely on `types.bindValue`
                // which writes to buffer. But we need to write the header first.
                // We assume default text/binary format logic in `bindValue`.

                // We need to reserve space? No, `bindValue` appends.
                // But we need to write the 'B' header.
                // We'll use a placeholder for length.
                buf.writeByteAssumeCapacity('B');
                const len_pos = buf.len();
                try buf.write(&.{ 0, 0, 0, 0 }); // length placeholder

                try buf.writeByte(0); // unnamed destination portal
                try buf.writeByte(0); // unnamed source statement

                try buf.writeIntBig(u16, @intCast(param_count)); // num formats
                const formats_start = buf.len();
                try buf.writeByteNTimes(0, param_count * 2); // placeholders

                try buf.writeIntBig(u16, @intCast(param_count)); // num params

                inline for (fields, 0..) |field, i| {
                    const format_pos = formats_start + (i * 2);
                    const oid = types.inferOid(field.type);
                    const val = @field(args, field.name);
                    try types.bindValue(field.type, oid, val, buf, format_pos);
                }

                // Result columns format codes.
                // We don't know the result columns yet!
                // So we ask for all text (0) or all binary (1).
                // Let's ask for all text (0) to be safe/compatible for now, as we don't have `describe` result.
                try buf.write(&.{ 0, 0 });

                // Patch length
                const total_len = buf.len() - len_pos;
                std.mem.writeInt(u32, buf.buf[len_pos..len_pos+4], @intCast(total_len), .big);
            }

            // 3. Describe Portal ('D')
            {
                const payload_len = 6; // 4 + 'P' + 0
                try buf.ensureUnusedCapacity(1 + payload_len);
                buf.writeByteAssumeCapacity('D');
                buf.writeIntBigAssumeCapacity(u32, payload_len);
                buf.writeByteAssumeCapacity('P'); // Describe Portal
                buf.writeByteAssumeCapacity(0);   // unnamed portal
            }

            // 4. Execute ('E')
            {
                const payload_len = 9;
                try buf.ensureUnusedCapacity(1 + payload_len);
                buf.writeByteAssumeCapacity('E');
                buf.writeIntBigAssumeCapacity(u32, payload_len);
                buf.writeByteAssumeCapacity(0); // unnamed portal
                buf.writeIntBigAssumeCapacity(u32, 0); // no row limit
            }
        }

        pub fn sync(self: *PipelineSession) !void {
            const buf = &self.conn._buf;
            // Sync ('S')
            try buf.write(&.{ 'S', 0, 0, 0, 4 });
            try self.conn.write(buf.string());
            buf.reset();
        }

        pub const Iterator = struct {
            session: *PipelineSession,

            // We need to keep state between next() calls because `Describe` tells us about
            // the *next* `DataRow` set.
            // We might have multiple results buffered.

            pub fn next(self: *Iterator) !?*Result {
                const conn = self.session.conn;
                while (true) {
                    const msg = try conn.read();
                    switch (msg.type) {
                        '1', '2' => {}, // ParseComplete, BindComplete - ignore
                        'T' => {
                            // RowDescription
                            // We need to parse this to prepare the Result state
                            const data = msg.data;
                            const column_count = std.mem.readInt(u16, data[0..2], .big);

                            const arena = try self.session.allocator.create(ArenaAllocator);
                            arena.* = ArenaAllocator.init(self.session.allocator);

                            // Allocate new state for this result using arena
                            var state = try Result.State.init(arena.allocator(), column_count);
                            // Populate state
                            try state.from(column_count, data, arena.allocator());

                            const result = try arena.allocator().create(Result);
                            result.* = .{
                                ._conn = conn,
                                ._arena = arena,
                                ._release_conn = false,
                                ._oids = state.oids[0..column_count],
                                ._values = state.values[0..column_count],
                                .column_names = state.names[0..column_count],
                                .number_of_columns = column_count,
                                ._expect_ready_for_query = false,
                            };
                            return result;
                        },
                        'n' => {
                            // NoData (from Describe) - usually for commands like INSERT/UPDATE without RETURNING
                            // We still return a Result, but with 0 columns.
                            const arena = try self.session.allocator.create(ArenaAllocator);
                            arena.* = ArenaAllocator.init(self.session.allocator);

                            const result = try arena.allocator().create(Result);
                            result.* = .{
                                ._conn = conn,
                                ._arena = arena,
                                ._release_conn = false,
                                ._oids = &[_]i32{},
                                ._values = &[_]lib.types.Value{},
                                .column_names = &[_][]const u8{},
                                .number_of_columns = 0,
                                ._expect_ready_for_query = false,
                            };
                            return result;
                        },
                        'C' => {
                            // CommandComplete without previous RowDescription/NoData?
                            // This happens if we skipped Describe?
                            // But we sent Describe. So we should get 'n' or 'T' before 'C'.
                            // UNLESS `Execute` was for an empty statement?
                            // Or `Describe` returns `NoData`.

                            // If we get 'C' here, it might be the end of the query execution for the *previous* result?
                            // No, `Result.next()` consumes 'C'.
                            // So if we returned a `Result` above (on 'T' or 'n'), the user would call `result.next()` until null.
                            // `result.next()` consumes 'D' (DataRow) until 'C' (CommandComplete).

                            // So `Iterator.next()` should ideally be called *after* the user is done with the previous Result.
                            // If the user didn't drain the previous result, `conn` is in a state where it has data.
                            // `conn.read()` would pick up 'D'.

                            // If `Iterator.next()` encounters 'D', it means the previous result wasn't drained.
                            // We should probably drain it?
                            // Or return error?
                            // Or `Result` should automatically drain in deinit?
                            // `src/result.zig` says "You must call drain".

                            // If we encounter 'Z', we are done.
                        },
                        'Z' => {
                            self.session.conn._state = switch (msg.data[0]) {
                                'I' => .idle,
                                'T' => .transaction,
                                'E' => .fail,
                                else => unreachable,
                            };
                            return null;
                        },
                        'E' => return conn.setErr(msg.data),
                        'N' => {
                            if (conn._notice_cb) |cb| {
                                const notice = proto.NoticeResponse.parse(msg.data);
                                cb({}, notice);
                            }
                        },
                        else => return conn.unexpectedDBMessage(),
                    }
                }
            }
        };

        pub fn results(self: *PipelineSession) Iterator {
            return Iterator{ .session = self };
        }
    };

    pub const CopySession = struct {
        conn: *Conn,
        direction: enum { in, out },
        format: u8, // 0 = text, 1 = binary
        columns: u16,
        column_formats: []u16,

        pub fn deinit(self: CopySession) void {
            self.conn._allocator.free(self.column_formats);
        }

        pub fn write(self: *CopySession, data: []const u8) !void {
            if (self.direction != .in) return error.WrongCopyDirection;
            var buf = &self.conn._buf;
            buf.reset();
            const msg = proto.CopyData{ .data = data };
            try msg.write(buf);
            try self.conn.write(buf.string());
        }

        pub fn done(self: *CopySession) !void {
            if (self.direction != .in) return error.WrongCopyDirection;
            var buf = &self.conn._buf;
            buf.reset();
            try proto.CopyDone.write(buf);
            try self.conn.write(buf.string());

            // Wait for CommandComplete
            while (true) {
                 const msg = self.conn.read() catch |err| {
                    if (err == error.PG) {
                        self.conn.readyForQuery() catch {};
                    }
                    return err;
                };
                switch (msg.type) {
                    'C' => {
                        // CommandComplete
                    },
                    'Z' => {
                        self.conn._state = switch (msg.data[0]) {
                            'I' => .idle,
                            'T' => .transaction,
                            'E' => .fail,
                            else => unreachable,
                        };
                        return;
                    },
                    'E' => return self.conn.setErr(msg.data),
                    'N' => {
                        if (self.conn._notice_cb) |cb| {
                            const notice = proto.NoticeResponse.parse(msg.data);
                            cb({}, notice);
                        }
                    },
                    'S' => {},
                    else => return self.conn.unexpectedDBMessage(),
                }
            }
        }

        pub fn fail(self: *CopySession, message: []const u8) !void {
             if (self.direction != .in) return error.WrongCopyDirection;
            var buf = &self.conn._buf;
            buf.reset();
            const msg = proto.CopyFail{ .message = message };
            try msg.write(buf);
            try self.conn.write(buf.string());
            // Expect ErrorResponse from server
             while (true) {
                 const msg = self.conn.read() catch |err| {
                    if (err == error.PG) {
                        self.conn.readyForQuery() catch {};
                    }
                    return err;
                };
                switch (msg.type) {
                    'Z' => {
                        self.conn._state = switch (msg.data[0]) {
                            'I' => .idle,
                            'T' => .transaction,
                            'E' => .fail,
                            else => unreachable,
                        };
                        return;
                    },
                    'N' => {
                        if (self.conn._notice_cb) |cb| {
                            const notice = proto.NoticeResponse.parse(msg.data);
                            cb({}, notice);
                        }
                    },
                    else => {}, // Drain everything else
                }
            }
        }

        pub fn next(self: *CopySession) !?[]const u8 {
            if (self.direction != .out) return error.WrongCopyDirection;
            const msg = try self.conn.read();
            switch (msg.type) {
                'd' => { // CopyData
                     // The data in msg.data is valid until the next call to read()
                     // But CopyData struct usually just wraps it.
                     // The proto.CopyData.parse is trivial
                     return msg.data;
                },
                'c' => { // CopyDone
                    // Expect CommandComplete then ReadyForQuery
                     while (true) {
                        const m = self.conn.read() catch |err| {
                            if (err == error.PG) {
                                self.conn.readyForQuery() catch {};
                            }
                            return err;
                        };
                         switch (m.type) {
                            'C' => {},
                            'Z' => {
                                self.conn._state = switch (m.data[0]) {
                                    'I' => .idle,
                                    'T' => .transaction,
                                    'E' => .fail,
                                    else => unreachable,
                                };
                                return null;
                            },
                            'E' => return self.conn.setErr(m.data),
                            'N' => {
                                if (self.conn._notice_cb) |cb| {
                                    const notice = proto.NoticeResponse.parse(m.data);
                                    cb({}, notice);
                                }
                            },
                            else => return self.conn.unexpectedDBMessage(),
                        }
                    }
                },
                 'E' => {
                     _ = self.conn.setErr(msg.data) catch {};
                     return error.PG;
                 },
                 'N' => {
                    if (self.conn._notice_cb) |cb| {
                        const notice = proto.NoticeResponse.parse(msg.data);
                        cb({}, notice);
                    }
                    // Recursive call to get next message
                    return self.next();
                 },
                else => return self.conn.unexpectedDBMessage(),
            }
        }
    };

    pub fn openAndAuthUri(allocator: Allocator, uri: std.Uri) !Conn {
        var po = try lib.parseOpts(uri, allocator);
        defer po.deinit();
        return try openAndAuth(allocator, po.opts.connect, po.opts.auth);
    }

    pub fn openAndAuth(allocator: Allocator, opts: Opts, ao: AuthOpts) !Conn {
        var conn = try open(allocator, opts);
        errdefer conn.deinit();

        try conn.auth(ao);
        return conn;
    }

    pub fn open(allocator: Allocator, opts: Opts) !Conn {
        var ssl_ctx: ?*SSLCtx = null;
        switch (opts.tls) {
            .off => {},
            else => |tls_config| {
                if (comptime lib.has_openssl == false) {
                    return error.OpenSSLNotConfigured;
                }
                ssl_ctx = try lib.initializeSSLContext(tls_config);
            },
        }
        errdefer lib.freeSSLContext(ssl_ctx);
        var conn = try openWithContext(allocator, opts, ssl_ctx);
        conn._ssl_ctx = ssl_ctx;
        return conn;
    }

    pub fn openWithContext(allocator: Allocator, opts: Opts, ssl_ctx: ?*SSLCtx) !Conn {
        var stream = try Stream.connect(allocator, opts, ssl_ctx);
        errdefer stream.close();

        const buf = try Buffer.init(allocator, @max(opts.write_buffer orelse 2048, 128));
        errdefer buf.deinit();

        const reader = try Reader.init(allocator, opts.read_buffer orelse 4096, stream);
        errdefer reader.deinit();

        const result_state = try Result.State.init(allocator, opts.result_state_size);
        errdefer result_state.deinit(allocator);

        const param_oids = try allocator.alloc(i32, opts.result_state_size);
        errdefer param_oids.deinit(allocator);

        // We need to store opts for cancellation.
        // Deep copy of host might be needed if it points to temporary memory.
        var stored_opts = opts;
        if (opts.host) |h| {
            stored_opts.host = try allocator.dupe(u8, h);
        }

        return .{
            .err = null,
            ._buf = buf,
            ._ssl_ctx = null,
            ._reader = reader,
            ._stream = stream,
            ._err_data = null,
            ._state = .idle,
            ._allocator = allocator,
            ._param_oids = param_oids,
            ._result_state = result_state,
            ._prepared_statements = .{},
            ._opts = stored_opts,
            ._notice_cb = opts.notice_cb,
        };
    }

    pub fn deinit(self: *Conn) void {
        const allocator = self._allocator;
        if (self._err_data) |err_data| {
            allocator.free(err_data);
        }
        if (self._opts.host) |h| {
            allocator.free(h);
        }
        self._buf.deinit();
        self._reader.deinit();
        allocator.free(self._param_oids);
        self._result_state.deinit(allocator);

        // try to send a Terminate to the DB
        self.write(&.{ 'X', 0, 0, 0, 4 }) catch {};
        lib.freeSSLContext(self._ssl_ctx);
        self._stream.close();

        var it = self._prepared_statements.valueIterator();
        while (it.next()) |value_ptr| {
            value_ptr.arena.deinit();
        }
        self._prepared_statements.deinit(self._allocator);
    }

    pub fn socketFd(self: *Conn) std.posix.socket_t {
        return self._stream.socket;
    }

    pub fn release(self: *Conn) void {
        var pool = self._pool orelse {
            self.deinit();
            return;
        };
        self.err = null;
        pool.release(self);
    }

    pub fn auth(self: *Conn, opts: AuthOpts) !void {
        if (try lib.auth.auth(&self._stream, &self._buf, &self._reader, opts)) |raw_pg_err| {
            return self.setErr(raw_pg_err);
        }

        while (true) {
            const msg = try self.read();
            switch (msg.type) {
                'Z' => return,
                'K' => {
                    // BackendKeyData
                    const key_data = try proto.BackendKeyData.parse(msg.data);
                    self._process_id = key_data.process_id;
                    self._secret_key = key_data.secret_key;
                },
                'N' => {
                    if (self._notice_cb) |cb| {
                        const notice = proto.NoticeResponse.parse(msg.data);
                        cb({}, notice);
                    }
                },
                'S' => {}, // ParameterStatus
                else => return self.unexpectedDBMessage(),
            }
        }
    }

    pub fn cancel(self: *Conn) !void {
        // Open a new connection using the stored options
        // We use a separate allocator for this temporary connection to avoid
        // messing with the main connection's allocator if possible, but
        // using the same one is also fine.
        var stream = try Stream.connect(self._allocator, self._opts, self._ssl_ctx);
        defer stream.close();

        var buf = try Buffer.init(self._allocator, 128);
        defer buf.deinit();

        const req = proto.CancelRequest{
            .process_id = self._process_id,
            .secret_key = self._secret_key,
        };
        try req.write(&buf);
        try stream.writeAll(buf.string());
    }

    pub fn prepare(self: *Conn, sql: []const u8) !Stmt {
        return self.prepareOpts(sql, .{});
    }

    pub fn prepareOpts(self: *Conn, sql: []const u8, opts: QueryOpts) !Stmt {
        var stmt = try Stmt.init(self, opts);
        errdefer stmt.deinit();
        try stmt.prepare(sql, null);
        return stmt;
    }

    pub fn prepareCached(self: *Conn, sql: []const u8, cache_name: []const u8) !void {
        if (self._prepared_statements.contains(cache_name)) {
            return;
        }
        var describe_arena = ArenaAllocator.init(self._allocator);
        errdefer describe_arena.deinit();

        var stmt = try Stmt.init(self, .{ .cache_name = cache_name });
        errdefer stmt.deinit();
        try stmt.prepare(sql, describe_arena.allocator());

        const owned_name = try describe_arena.allocator().dupe(u8, cache_name);
        try self._prepared_statements.put(self._allocator, owned_name, .{
            .arena = describe_arena,
            .param_oids = stmt.param_oids,
            .result_state = stmt.result_state,
        });
        stmt.deinit();
    }

    pub fn getPreparedDescribe(self: *Conn, name: []const u8) ?*Stmt.Describe {
        return self._prepared_statements.getPtr(name);
    }

    pub fn pipeline(self: *Conn) !PipelineSession {
        if (self.canQuery() == false) {
            return error.ConnectionBusy;
        }
        self._buf.reset();
        self._state = .query;
        return PipelineSession{ .conn = self, .allocator = self._allocator };
    }

    pub fn copy(self: *Conn, sql: []const u8) !CopySession {
         if (self.canQuery() == false) {
            return error.ConnectionBusy;
        }

        var buf = &self._buf;
        buf.reset();

        try self._reader.startFlow(self._allocator, null);

        const simple_query = proto.Query{ .sql = sql };
        try simple_query.write(buf);
        lib.metrics.query();
        self._state = .query;
        try self.write(buf.string());

        while(true) {
             const msg = self.read() catch |err| {
                if (err == error.PG) {
                    self.readyForQuery() catch {};
                }
                return err;
            };

            switch (msg.type) {
                'G' => { // CopyInResponse
                    const res = try proto.CopyInResponse.parse(self._allocator, msg.data);
                    self._state = .copy_in;
                    return CopySession{
                        .conn = self,
                        .direction = .in,
                        .format = res.format,
                        .columns = res.columns,
                        .column_formats = res.column_formats,
                    };
                },
                'H' => { // CopyOutResponse
                    const res = try proto.CopyOutResponse.parse(self._allocator, msg.data);
                    self._state = .copy_out;
                     return CopySession{
                        .conn = self,
                        .direction = .out,
                        .format = res.format,
                        .columns = res.columns,
                        .column_formats = res.column_formats,
                    };
                },
                'E' => {
                     _ = try self.setErr(msg.data);
                     return error.PG;
                },
                'N' => {
                    if (self._notice_cb) |cb| {
                        const notice = proto.NoticeResponse.parse(msg.data);
                        cb({}, notice);
                    }
                },
                else => return self.unexpectedDBMessage(),
            }
        }
    }

    pub fn query(self: *Conn, sql: []const u8, values: anytype) !*Result {
        return self.queryOpts(sql, values, .{});
    }

    pub fn queryOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !*Result {
        if (self.canQuery() == false) {
            self.maybeRelease(opts.release_conn);
            return error.ConnectionBusy;
        }

        var cached = false;
        var stmt: Stmt = undefined;
        const name = opts.cache_name;

        if (name) |n| {
            if (self._prepared_statements.getPtr(n)) |describe| {
                cached = true;
                stmt = try Stmt.fromDescribe(self, describe, opts);
                errdefer stmt.deinit();

                try self._reader.startFlow(stmt.arena.allocator(), opts.timeout);
                // Send a "SYNC" command
                try self.write(&.{ 'S', 0, 0, 0, 4 });
                stmt.buf.reset();
                try stmt.prepareForBind(@intCast(describe.param_oids.len));
            }
        }

        if (cached == false) {
            // either this isn't supposed to be cached, or it is, but we don't
            // have it in our cache
            stmt = Stmt.init(self, opts) catch |err| {
                self.maybeRelease(opts.release_conn);
                return err;
            };

            errdefer stmt.deinit();
            if (name) |n| {
                var describe_arena = ArenaAllocator.init(self._allocator);
                errdefer describe_arena.deinit();
                try stmt.prepare(sql, describe_arena.allocator());

                // When prepare is called with our describe arena, than its
                // param_oids and result_state will be create with it specifically
                // so that we can copy them here.
                const owned_name = try describe_arena.allocator().dupe(u8, n);
                try self._prepared_statements.put(self._allocator, owned_name, .{
                    .arena = describe_arena,
                    .param_oids = stmt.param_oids,
                    .result_state = stmt.result_state,
                });
            } else {
                try stmt.prepare(sql, null);
            }
        }

        {
            errdefer stmt.deinit();
            if (values.len != stmt.param_count) {
                return error.WrongNumberOfParameters;
            }

            inline for (values) |value| {
                try stmt.bind(value);
            }
        }

        return stmt.execute() catch |err| {
            stmt.deinit();
            self.maybeRelease(opts.release_conn);
            return err;
        };
    }

    pub fn row(self: *Conn, sql: []const u8, values: anytype) !?QueryRow {
        return self.rowOpts(sql, values, .{});
    }

    pub fn rowOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !?QueryRow {
        var result = try self.queryOpts(sql, values, opts);
        errdefer result.deinit();

        const r = try result.next() orelse {
            result.deinit();
            return null;
        };

        return .{
            .row = r,
            .result = result,
        };
    }

    // Execute a query that does not return rows
    pub fn exec(self: *Conn, sql: []const u8, values: anytype) !?i64 {
        return self.execOpts(sql, values, .{});
    }

    pub fn execOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !?i64 {
        if (self.canQuery() == false) {
            return error.ConnectionBusy;
        }
        var buf = &self._buf;
        buf.reset();

        if (values.len == 0) {
            try self._reader.startFlow(opts.allocator, opts.timeout);
            defer self._reader.endFlow() catch {
                // this can only fail in extreme conditions (OOM) and it will only impact
                // the next query (and if the app is using the pool, the pool will try to
                // recover from this anyways)
                self._state = .fail;
            };
            const simple_query = proto.Query{ .sql = sql };
            try simple_query.write(buf);
            // no longer idle, we're now in a query
            lib.metrics.query();
            self._state = .query;
            try self.write(buf.string());
        } else {
            // TODO: there's some optimization opportunities here, since we know
            // we aren't expecting any result. We don't have to ask PG to DESCRIBE
            // the returned columns (there should be none). This is very significant
            // as it would remove 1 back-and-forth. We could just:
            //    Parse + Bind + Exec + Sync
            // Instead of having to do:
            //    Parse + Describe + Sync  ... read response ...  Bind + Exec + Sync
            const result = try self.queryOpts(sql, values, opts);
            result.deinit();
        }

        // affected can be null, so we need a separate boolean to track if we
        // actually have a response.
        var affected: ?i64 = null;
        while (true) {
            const msg = self.read() catch |err| {
                if (err == error.PG) {
                    self.readyForQuery() catch {};
                }
                return err;
            };
            switch (msg.type) {
                'C' => {
                    const cc = try proto.CommandComplete.parse(msg.data);
                    affected = cc.rowsAffected();
                },
                'Z' => return affected,
                'T' => affected = 0,
                'D' => affected = (affected orelse 0) + 1,
                'N' => {
                    if (self._notice_cb) |cb| {
                        const notice = proto.NoticeResponse.parse(msg.data);
                        cb({}, notice);
                    }
                },
                else => return self.unexpectedDBMessage(),
            }
        }
    }

    pub fn begin(self: *Conn) !void {
        self._state = .transaction;
        _ = try self.execOpts("begin", .{}, .{});
    }

    pub fn commit(self: *Conn) !void {
        _ = try self.execOpts("commit", .{}, .{});
    }

    // We don't use `execOpts` here because rollback can be called at any point
    // and we want to send this command even if the conn is in a fail state.
    // So we issue the rollback, no matter what state we're in.
    // It's also possible rollback was called while we were reading results,
    // so we need to keep reading replies until we get a ready to query state,
    // just skipping over any data rows or any other in-flight messages there
    // might be.
    pub fn rollback(self: *Conn) !void {
        var buf = &self._buf;
        buf.reset();

        const state = self._state;

        const simple_query = proto.Query{ .sql = "rollback" };
        try simple_query.write(buf);
        try self.write(buf.string());
        while (true) {
            const msg = self.read() catch |err| {
                if (state != .fail and err == error.PG) {
                    self.readyForQuery() catch {};
                }
                return err;
            };
            switch (msg.type) {
                'Z' => return,
                'C', 'T', 'D' => {},
                'N' => {
                    if (self._notice_cb) |cb| {
                        const notice = proto.NoticeResponse.parse(msg.data);
                        cb({}, notice);
                    }
                },
                else => return self.unexpectedDBMessage(),
            }
        }
    }

    pub fn deallocate(self: *Conn, cache_name: []const u8) !void {
        if (self._prepared_statements.fetchRemove(cache_name)) |kv| {
            kv.value.arena.deinit();
        }
        const allocator = self._allocator;
        const sql = try std.fmt.allocPrint(allocator, "deallocate {s}", .{cache_name});
        defer allocator.free(sql);
        _ = try self.execOpts(sql, .{}, .{});
    }

    // Should not be called directly
    pub fn peekForError(self: *Conn) !void {
        const data = (try self._reader.peekForError()) orelse return;
        try self.readyForQuery();
        return self.setErr(data);
    }

    // Should not be called directly
    pub fn read(self: *Conn) !lib.Message {
        var reader = &self._reader;
        while (true) {
            const msg = reader.next() catch |err| {
                self._state = .fail;
                return err;
            };
            switch (msg.type) {
                'Z' => {
                    self._state = switch (msg.data[0]) {
                        'I' => .idle,
                        'T' => .transaction,
                        'E' => .fail,
                        else => unreachable,
                    };
                    return msg;
                },
                'S' => {}, // TODO: ParameterStatus,
                'N' => {
                    if (self._notice_cb) |cb| {
                        const notice = proto.NoticeResponse.parse(msg.data);
                        cb({}, notice);
                    }
                },
                'E' => return self.setErr(msg.data),
                else => return msg,
            }
        }
    }

    pub fn readNonBlocking(self: *Conn) !lib.Message {
        var reader = &self._reader;
        while (true) {
            const msg = reader.nextNonBlocking() catch |err| switch (err) {
                error.WouldBlock => return error.WouldBlock,
                else => {
                    self._state = .fail;
                    return err;
                },
            };
            switch (msg.type) {
                'Z' => {
                    self._state = switch (msg.data[0]) {
                        'I' => .idle,
                        'T' => .transaction,
                        'E' => .fail,
                        else => unreachable,
                    };
                    return msg;
                },
                'S' => {},
                'N' => {
                    if (self._notice_cb) |cb| {
                        const notice = proto.NoticeResponse.parse(msg.data);
                        cb({}, notice);
                    }
                },
                'E' => return self.setErr(msg.data),
                else => return msg,
            }
        }
    }

    pub fn write(self: *Conn, data: []const u8) !void {
        self._stream.writeAll(data) catch |err| {
            self._state = .fail;
            return err;
        };
    }

    pub fn writeNonBlocking(self: *Conn, data: []const u8) !usize {
        const n = self._stream.writeNonBlocking(data) catch |err| {
            if (err != error.WouldBlock) {
                self._state = .fail;
            }
            return err;
        };
        return n;
    }

    fn setErr(self: *Conn, data: []const u8) error{ PG, OutOfMemory } {
        const allocator = self._allocator;

        // The proto.Error that we're about to create is going to reference data.
        // But data is owned by our Reader and its lifetime doesn't necessarily match
        // what we want here. So we're going to dupe it and make the connection own
        // the data so it can tie its lifecycle to the error.

        // That means clearing out any previous duped error data we had
        if (self._err_data) |err_data| {
            allocator.free(err_data);
        }

        const owned = try allocator.dupe(u8, data);
        self._err_data = owned;
        self.err = proto.Error.parse(owned);
        return error.PG;
    }

    pub fn unexpectedDBMessage(self: *Conn) error{UnexpectedDBMessage} {
        self._state = .fail;
        return error.UnexpectedDBMessage;
    }

    fn canQuery(self: *const Conn) bool {
        const state = self._state;
        if (state == .idle or state == .transaction) {
            return true;
        }
        return false;
    }

    inline fn maybeRelease(self: *Conn, rel: bool) void {
        if (rel) {
            self.release();
        }
    }

    // should not be called directly
    pub fn readyForQuery(self: *Conn) !void {
        const msg = try self.read();
        if (msg.type != 'Z') {
            return self.unexpectedDBMessage();
        }
    }
};

const t = lib.testing;
test "Conn: auth trust (no pass)" {
    var conn = try Conn.open(t.allocator, .{});
    defer conn.deinit();
    try conn.auth(.{ .username = "pgz_user_nopass", .database = "postgres" });
}

test "Conn: auth unknown user" {
    var conn = try Conn.open(t.allocator, .{});
    defer conn.deinit();
    try t.expectError(error.PG, conn.auth(.{ .username = "does_not_exist" }));
    try t.expectEqual(true, std.mem.indexOf(u8, conn.err.?.message, "user \"does_not_exist\"") != null);
}

test "Conn: auth cleartext password" {
    {
        var conn = try Conn.open(t.allocator, .{});
        defer conn.deinit();
        try t.expectError(error.PG, conn.auth(.{ .username = "pgz_user_clear" }));
        try t.expectString("empty password returned by client", conn.err.?.message);
    }

    {
        var conn = try Conn.open(t.allocator, .{});
        defer conn.deinit();
        try t.expectError(error.PG, conn.auth(.{ .username = "pgz_user_clear", .password = "wrong" }));
        try t.expectString("password authentication failed for user \"pgz_user_clear\"", conn.err.?.message);
    }

    {
        var conn = try Conn.open(t.allocator, .{});
        defer conn.deinit();
        try conn.auth(.{ .username = "pgz_user_clear", .password = "pgz_user_clear_pw", .database = "postgres" });
    }
}

test "Conn: auth scram-sha-256 password" {
    {
        var conn = try Conn.open(t.allocator, .{});
        defer conn.deinit();
        try t.expectError(error.PG, conn.auth(.{ .username = "pgz_user_scram_sha256" }));
        try t.expectString("password authentication failed for user \"pgz_user_scram_sha256\"", conn.err.?.message);
    }

    {
        var conn = try Conn.open(t.allocator, .{});
        defer conn.deinit();
        try t.expectError(error.PG, conn.auth(.{ .username = "pgz_user_scram_sha256", .password = "wrong" }));
        try t.expectString("password authentication failed for user \"pgz_user_scram_sha256\"", conn.err.?.message);
    }

    {
        var conn = try Conn.open(t.allocator, .{});
        defer conn.deinit();
        try conn.auth(.{ .username = "pgz_user_scram_sha256", .password = "pgz_user_scram_sha256_pw", .database = "postgres" });
    }
}

test "Conn: exec rowsAffected" {
    var c = t.connect(.{});
    defer c.deinit();

    {
        const n = try c.exec("insert into simple_table values ('exec_insert_a'), ('exec_insert_b')", .{});
        try t.expectEqual(2, n.?);
    }

    {
        const n = try c.exec("update simple_table set value = 'exec_insert_a' where value = 'exec_insert_a'", .{});
        try t.expectEqual(1, n.?);
    }

    {
        const n = try c.exec("delete from simple_table where value like 'exec_insert%'", .{});
        try t.expectEqual(2, n.?);
    }

    {
        try t.expectEqual(null, try c.exec("begin", .{}));
        try t.expectEqual(null, try c.exec("end", .{}));
    }
}

test "Conn: exec with values rowsAffected" {
    var c = t.connect(.{});
    defer c.deinit();

    {
        const n = try c.exec("insert into simple_table values ($1), ($2)", .{ "exec_insert_args_a", "exec_insert_args_b" });
        try t.expectEqual(2, n.?);
    }
}

test "Conn: exec query that returns rows" {
    var c = t.connect(.{});
    defer c.deinit();
    _ = try c.exec("insert into simple_table values ('exec_sel_1'), ('exec_sel_2')", .{});
    try t.expectEqual(0, c.exec("select * from simple_table where value = 'none'", .{}));
    try t.expectEqual(2, c.exec("select * from simple_table where value like $1", .{"exec_sel_%"}));
}

test "Conn: parse error" {
    var c = t.connect(.{});
    defer c.deinit();
    try t.expectError(error.PG, c.query("selct 1", .{}));

    const err = c.err.?;
    try t.expectString("42601", err.code);
    try t.expectString("ERROR", err.severity);
    try t.expectString("syntax error at or near \"selct\"", err.message);

    // connection is still usable
    try t.expectEqual(2, t.scalar(&c, "select 2"));
}

test "Conn: Query within Query error" {
    var c = t.connect(.{});
    defer c.deinit();
    var rows = try c.query("select 1", .{});
    defer rows.deinit();

    try t.expectError(error.ConnectionBusy, c.row("select 2", .{}));
    try t.expectEqual(1, (try rows.next()).?.get(i32, 0));
}

test "PG: type support" {
    defer t.reset();

    var c = t.connect(.{});
    defer c.deinit();
    var bytea1 = [_]u8{ 0, 1 };
    var bytea2 = [_]u8{ 255, 253, 253 };

    {
        const result = c.exec(
            \\
            \\ insert into all_types (
            \\   id,
            \\   col_int2, col_int2_arr,
            \\   col_int4, col_int4_arr,
            \\   col_int8, col_int8_arr,
            \\   col_float4, col_float4_arr,
            \\   col_float8, col_float8_arr,
            \\   col_bool, col_bool_arr,
            \\   col_text, col_text_arr,
            \\   col_bytea, col_bytea_arr,
            \\   col_enum, col_enum_arr,
            \\   col_uuid, col_uuid_arr,
            \\   col_numeric, col_numeric_arr,
            \\   col_timestamp, col_timestamp_arr,
            \\   col_timestamptz, col_timestamptz_arr,
            \\   col_json, col_json_arr,
            \\   col_jsonb, col_jsonb_arr,
            \\   col_char, col_char_arr,
            \\   col_charn, col_charn_arr,
            \\   col_cidr, col_cidr_arr,
            \\   col_inet, col_inet_arr,
            \\   col_macaddr, col_macaddr_arr,
            \\   col_macaddr8, col_macaddr8_arr
            \\ ) values (
            \\   $1,
            \\   $2, $3,
            \\   $4, $5,
            \\   $6, $7,
            \\   $8, $9,
            \\   $10, $11,
            \\   $12, $13,
            \\   $14, $15,
            \\   $16, $17,
            \\   $18, $19,
            \\   $20, $21,
            \\   $22, $23,
            \\   $24, $25,
            \\   $26, $27,
            \\   $28, $29,
            \\   $30, $31,
            \\   $32, $33,
            \\   $34, $35,
            \\   $36, $37,
            \\   $38, $39,
            \\   $40, $41,
            \\   $42, $43
            \\ )
        , .{
            1,
            @as(i16, 382),
            [_]i16{ -9000, 9001 },
            @as(i32, -96534),
            [_]i32{-4929123},
            @as(i64, 8983919283),
            [_]i64{ 8888848483, 0, -1 },
            @as(f32, 1.2345),
            [_]f32{ 4.492, -0.000021 },
            @as(f64, -48832.3233231),
            [_]f64{ 393.291133, 3.1144 },
            true,
            [_]bool{ false, true },
            "a text column",
            [_][]const u8{ "it's", "over", "9000" },
            [_]u8{ 0, 0, 2, 255, 255, 255 },
            &[_][]u8{ &bytea1, &bytea2 },
            "val1",
            [_][]const u8{ "val1", "val2" },
            "b7cc282f-ec43-49be-8e09-aafab0104915",
            [_][]const u8{ "166B4751-D702-4FB9-9A2A-CD6B69ED18D6", "ae2f475f-8070-41b7-ba33-86bba8897bde" },
            1234.567,
            [_]f64{ 0, -1.1, std.math.nan(f64), std.math.inf(f32), 12345.000101 },
            "2023-10-23T15:33:13Z",
            [_][]const u8{ "2010-02-10T08:22:07Z", "0003-04-05T06:07:08.123456" },
            "2024-11-23T16:34:14Z",
            [_][]const u8{ "2011-03-11T09:23:05Z", "0002-03-04T05:06:02.0000991" },
            "{\"count\":1.3}",
            [_][]const u8{ "[1,2,3]", "{\"rows\":[{\"a\": true}]}" },
            "{\"over\":9000}",
            [_][]const u8{ "[true,false]", "{\"cols\":[{\"z\": 0.003}]}" },
            79,
            [_]u8{ '1', 'z', '!' },
            "Teg",
            [_][]const u8{ &.{ 78, 82 }, "hi" },
            "192.168.100.128/25",
            [_][]const u8{ "10.1.2", "2001:4f8:3:ba::/64" },
            "::ffff:1.2.3.0/120",
            [_][]const u8{ "127.0.0.1/32", "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128" },
            "08:00:2b:01:02:03",
            [_][]const u8{ "08002b:010203", "0800-2b01-0204" },
            "09:01:3b:21:21:03:04:05",
            [_][]const u8{ "ffeeddccbbaa9988", "01-02-03-04-05-06-07-09" },
        });
        if (result) |affected| {
            try t.expectEqual(1, affected);
        } else |err| {
            try t.fail(c, err);
        }
    }

    var result = try c.query(
        \\ select
        \\   id,
        \\   col_int2, col_int2_arr,
        \\   col_int4, col_int4_arr,
        \\   col_int8, col_int8_arr,
        \\   col_float4, col_float4_arr,
        \\   col_float8, col_float8_arr,
        \\   col_bool, col_bool_arr,
        \\   col_text, col_text_arr,
        \\   col_bytea, col_bytea_arr,
        \\   col_enum, col_enum_arr,
        \\   col_uuid, col_uuid_arr,
        \\   col_numeric, col_numeric_arr,
        \\   col_timestamp, col_timestamp_arr,
        \\   col_timestamptz, col_timestamptz_arr,
        \\   col_json, col_json_arr,
        \\   col_jsonb, col_jsonb_arr,
        \\   col_char, col_char_arr,
        \\   col_charn, col_charn_arr,
        \\   col_cidr, col_cidr_arr,
        \\   col_inet, col_inet_arr,
        \\   col_macaddr, col_macaddr_arr,
        \\   col_macaddr8, col_macaddr8_arr
        \\ from all_types where id = $1
    , .{1});
    defer result.deinit();

    // used for our arrays
    const aa = t.arena.allocator();

    const row = (try result.next()) orelse unreachable;
    try t.expectEqual(1, row.get(i32, 0));

    {
        // smallint & smallint[]
        try t.expectEqual(382, row.get(i16, 1));
        try t.expectSlice(i16, &.{ -9000, 9001 }, try row.iterator(i16, 2).alloc(aa));
    }

    {
        // int & int[]
        try t.expectEqual(-96534, row.get(i32, 3));
        try t.expectSlice(i32, &.{-4929123}, try row.iterator(i32, 4).alloc(aa));
    }

    {
        // bigint & bigint[]
        try t.expectEqual(8983919283, row.get(i64, 5));
        try t.expectSlice(i64, &.{ 8888848483, 0, -1 }, try row.iterator(i64, 6).alloc(aa));
    }

    {
        // float4, float4[]
        try t.expectEqual(1.2345, row.get(f32, 7));
        try t.expectSlice(f32, &.{ 4.492, -0.000021 }, try row.iterator(f32, 8).alloc(aa));
    }

    {
        // float8, float8[]
        try t.expectEqual(-48832.3233231, row.get(f64, 9));
        try t.expectSlice(f64, &.{ 393.291133, 3.1144 }, try row.iterator(f64, 10).alloc(aa));
    }

    {
        // bool, bool[]
        try t.expectEqual(true, row.get(bool, 11));
        try t.expectSlice(bool, &.{ false, true }, try row.iterator(bool, 12).alloc(aa));
    }

    {
        // text, text[]
        try t.expectString("a text column", row.get([]u8, 13));
        const arr = try row.iterator([]const u8, 14).alloc(aa);
        try t.expectEqual(3, arr.len);
        try t.expectString("it's", arr[0]);
        try t.expectString("over", arr[1]);
        try t.expectString("9000", arr[2]);
    }

    {
        // bytea, bytea[]
        try t.expectSlice(u8, &.{ 0, 0, 2, 255, 255, 255 }, row.get([]const u8, 15));
        const arr = try row.iterator([]u8, 16).alloc(aa);
        try t.expectEqual(2, arr.len);
        try t.expectSlice(u8, &bytea1, arr[0]);
        try t.expectSlice(u8, &bytea2, arr[1]);
    }

    {
        // enum, emum[]
        try t.expectString("val1", row.get([]u8, 17));
        const arr = try row.iterator([]const u8, 18).alloc(aa);
        try t.expectEqual(2, arr.len);
        try t.expectString("val1", arr[0]);
        try t.expectString("val2", arr[1]);
    }

    {
        //uuid, uuid[]
        try t.expectSlice(u8, &.{ 183, 204, 40, 47, 236, 67, 73, 190, 142, 9, 170, 250, 176, 16, 73, 21 }, row.get([]u8, 19));
        const arr = try row.iterator([]const u8, 20).alloc(aa);
        try t.expectEqual(2, arr.len);
        try t.expectSlice(u8, &.{ 22, 107, 71, 81, 215, 2, 79, 185, 154, 42, 205, 107, 105, 237, 24, 214 }, arr[0]);
        try t.expectSlice(u8, &.{ 174, 47, 71, 95, 128, 112, 65, 183, 186, 51, 134, 187, 168, 137, 123, 222 }, arr[1]);
    }

    {
        // numeric, numeric[]
        try t.expectEqual(1234.567, row.get(f64, 21));
        const arr = try row.iterator(types.Numeric, 22).alloc(aa);
        try t.expectEqual(5, arr.len);
        try expectNumeric(arr[0], "0.0");
        try expectNumeric(arr[1], "-1.1");
        try expectNumeric(arr[2], "nan");
        try expectNumeric(arr[3], "inf");
        try expectNumeric(arr[4], "12345.000101");
    }

    {
        //timestamp, timestamp[]
        try t.expectEqual(1698075193000000, row.get(i64, 23));
        try t.expectSlice(i64, &.{ 1265790127000000, -62064381171876544 }, try row.iterator(i64, 24).alloc(aa));
    }

    {
        //timestamptz, timestamptz[]
        try t.expectEqual(1732379654000000, row.get(i64, 25));
        try t.expectSlice(i64, &.{ 1299835385000000, -62098685637999901 }, try row.iterator(i64, 26).alloc(aa));
    }

    {
        // json, json[]
        try t.expectString("{\"count\":1.3}", row.get([]u8, 27));
        const arr = try row.iterator([]const u8, 28).alloc(aa);
        try t.expectEqual(2, arr.len);
        try t.expectString("[1,2,3]", arr[0]);
        try t.expectString("{\"rows\":[{\"a\": true}]}", arr[1]);
    }

    {
        // jsonb, jsonb[]
        try t.expectString("{\"over\": 9000}", row.get([]u8, 29));
        const arr = try row.iterator([]const u8, 30).alloc(aa);
        try t.expectEqual(2, arr.len);
        try t.expectString("[true, false]", arr[0]);
        try t.expectString("{\"cols\": [{\"z\": 0.003}]}", arr[1]);
    }

    {
        // char, char[]
        try t.expectEqual(79, row.get(u8, 31));
        const arr = try row.iterator(u8, 32).alloc(aa);
        try t.expectEqual(3, arr.len);
        try t.expectEqual('1', arr[0]);
        try t.expectEqual('z', arr[1]);
        try t.expectEqual('!', arr[2]);
    }

    {
        // charn, charn[]
        try t.expectString("Teg", row.get([]u8, 33));
        const arr = try row.iterator([]u8, 34).alloc(aa);
        try t.expectEqual(2, arr.len);
        try t.expectString("NR", arr[0]);
        try t.expectString("hi", arr[1]);
    }

    {
        // cidr, cidr[]
        const cidr = row.get(types.Cidr, 35);
        try t.expectEqual(25, cidr.netmask);
        try t.expectEqual(.v4, cidr.family);
        try t.expectString(&.{ 192, 168, 100, 128 }, cidr.address);

        const arr = try row.iterator(types.Cidr, 36).alloc(aa);
        try t.expectEqual(2, arr.len);

        try t.expectEqual(24, arr[0].netmask);
        try t.expectEqual(.v4, arr[0].family);
        try t.expectString(&.{ 10, 1, 2, 0 }, arr[0].address);

        try t.expectEqual(64, arr[1].netmask);
        try t.expectEqual(.v6, arr[1].family);
        try t.expectSlice(u8, &.{ 32, 1, 4, 248, 0, 3, 0, 186, 0, 0, 0, 0, 0, 0, 0, 0 }, arr[1].address);
    }

    {
        // inet, inet[]
        const inet = row.get(types.Cidr, 37);
        try t.expectEqual(120, inet.netmask);
        try t.expectEqual(.v6, inet.family);
        try t.expectString(&.{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 1, 2, 3, 0 }, inet.address);

        const arr = try row.iterator(types.Cidr, 38).alloc(aa);
        try t.expectEqual(2, arr.len);

        try t.expectEqual(32, arr[0].netmask);
        try t.expectEqual(.v4, arr[0].family);
        try t.expectString(&.{ 127, 0, 0, 1 }, arr[0].address);

        try t.expectEqual(128, arr[1].netmask);
        try t.expectEqual(.v6, arr[1].family);
        try t.expectSlice(u8, &.{ 32, 1, 4, 248, 0, 3, 0, 186, 2, 224, 129, 255, 254, 34, 209, 241 }, arr[1].address);
    }

    {
        // macaddr, macaddr[]
        try t.expectSlice(u8, &.{ 8, 0, 43, 1, 2, 3 }, row.get([]u8, 39));

        const arr = try row.iterator([]u8, 40).alloc(aa);
        try t.expectEqual(2, arr.len);
        try t.expectSlice(u8, &.{ 8, 0, 43, 1, 2, 3 }, arr[0]);
        try t.expectSlice(u8, &.{ 8, 0, 43, 1, 2, 4 }, arr[1]);
    }

    {
        // macaddr8, macaddr8[]
        try t.expectSlice(u8, &.{ 9, 1, 59, 33, 33, 3, 4, 5 }, row.get([]u8, 41));

        const arr = try row.iterator([]u8, 42).alloc(aa);
        try t.expectEqual(2, arr.len);
        try t.expectSlice(u8, &.{ 255, 238, 221, 204, 187, 170, 153, 136 }, arr[0]);
        try t.expectSlice(u8, &.{ 1, 2, 3, 4, 5, 6, 7, 9 }, arr[1]);
    }

    try t.expectEqual(null, try result.next());
}

test "PG: null support" {
    var c = t.connect(.{});
    defer c.deinit();
    {
        const result = c.exec(
            \\
            \\ insert into all_types (
            \\   id,
            \\   col_int2, col_int2_arr,
            \\   col_int4, col_int4_arr,
            \\   col_int8, col_int8_arr,
            \\   col_float4, col_float4_arr,
            \\   col_float8, col_float8_arr,
            \\   col_bool, col_bool_arr,
            \\   col_text, col_text_arr,
            \\   col_bytea, col_bytea_arr,
            \\   col_enum, col_enum_arr,
            \\   col_uuid, col_uuid_arr,
            \\   col_numeric, col_numeric_arr,
            \\   col_timestamp, col_timestamp_arr,
            \\   col_json, col_json_arr,
            \\   col_jsonb, col_jsonb_arr,
            \\   col_char, col_char_arr,
            \\   col_charn, col_charn_arr
            \\ ) values (
            \\   $1,
            \\   $2, $3,
            \\   $4, $5,
            \\   $6, $7,
            \\   $8, $9,
            \\   $10, $11,
            \\   $12, $13,
            \\   $14, $15,
            \\   $16, $17,
            \\   $18, $19,
            \\   $20, $21,
            \\   $22, $23,
            \\   $24, $25,
            \\   $26, $27,
            \\   $28, $29,
            \\   $30, $31,
            \\   $32, $33
            \\ )
        , .{
            3,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
        });
        if (result) |affected| {
            try t.expectEqual(1, affected);
        } else |err| {
            try t.fail(c, err);
        }
    }

    var result = try c.query(
        \\ select
        \\   id,
        \\   col_int2, col_int2_arr,
        \\   col_int4, col_int4_arr,
        \\   col_int8, col_int8_arr,
        \\   col_float4, col_float4_arr,
        \\   col_float8, col_float8_arr,
        \\   col_bool, col_bool_arr,
        \\   col_text, col_text_arr,
        \\   col_bytea, col_bytea_arr,
        \\   col_enum, col_enum_arr,
        \\   col_uuid, col_uuid_arr,
        \\   col_numeric, 'numeric[] placeholder',
        \\   col_timestamp, col_timestamp_arr,
        \\   col_json, col_json_arr,
        \\   col_jsonb, col_jsonb_arr,
        \\   col_char, col_char_arr,
        \\   col_charn, col_charn_arr
        \\ from all_types where id = $1
    , .{3});
    defer result.deinit();

    const row = (try result.next()) orelse unreachable;
    try t.expectEqual(null, row.get(?i16, 1));
    try t.expectEqual(true, row.iterator(i16, 2).is_null);

    try t.expectEqual(null, row.get(?i32, 3));
    try t.expectEqual(true, row.iterator(i32, 4).is_null);

    try t.expectEqual(null, row.get(?i64, 5));
    try t.expectEqual(true, row.iterator(i64, 6).is_null);

    try t.expectEqual(null, row.get(?f32, 7));
    try t.expectEqual(true, row.iterator(f32, 8).is_null);

    try t.expectEqual(null, row.get(?f64, 9));
    try t.expectEqual(true, row.iterator(f64, 10).is_null);

    try t.expectEqual(null, row.get(?bool, 11));
    try t.expectEqual(true, row.iterator(bool, 12).is_null);

    try t.expectEqual(null, row.get(?[]u8, 13));
    try t.expectEqual(true, row.iterator([]u8, 14).is_null);

    try t.expectEqual(null, row.get(?[]const u8, 15));
    try t.expectEqual(true, row.iterator([]const u8, 16).is_null);

    try t.expectEqual(null, row.get(?[]const u8, 17));
    try t.expectEqual(true, row.iterator([]const u8, 18).is_null);

    try t.expectEqual(null, row.get(?[]u8, 19));
    try t.expectEqual(true, row.iterator([]const u8, 20).is_null);

    try t.expectEqual(null, row.get(?[]u8, 21));
    try t.expectEqual(null, row.get(?f64, 21));

    try t.expectEqual(null, row.get(?i64, 23));
    try t.expectEqual(true, row.iterator(i64, 24).is_null);

    try t.expectEqual(null, row.get(?[]u8, 25));
    try t.expectEqual(true, row.iterator([]const u8, 26).is_null);

    try t.expectEqual(null, row.get(?[]u8, 27));
    try t.expectEqual(true, row.iterator([]const u8, 28).is_null);

    try t.expectEqual(null, row.get(?u8, 29));
    try t.expectEqual(true, row.iterator(u8, 30).is_null);

    try t.expectEqual(null, row.get(?u8, 31));
    try t.expectEqual(true, row.iterator(u8, 32).is_null);

    try t.expectEqual(null, try result.next());
}

test "PG: query column names" {
    var c = t.connect(.{});
    defer c.deinit();
    {
        var result = try c.query("select 1 as id, 'leto' as name", .{});
        try t.expectEqual(0, result.column_names.len);
        try result.drain();
        result.deinit();
    }

    {
        var result = try c.queryOpts("select 1 as id, 'leto' as name", .{}, .{ .column_names = true });
        defer result.deinit();
        try t.expectEqual(2, result.column_names.len);
        try t.expectString("id", result.column_names[0]);
        try t.expectString("name", result.column_names[1]);
    }
}

test "PG: JSON struct" {
    var c = t.connect(.{});
    defer c.deinit();

    {
        const result = c.exec(
            \\
            \\ insert into all_types (id, col_json, col_jsonb)
            \\ values ($1, $2, $3)
        , .{ 4, DummyStruct{ .id = 1, .name = "Leto" }, &DummyStruct{ .id = 2, .name = "Ghanima" } });

        if (result) |affected| {
            try t.expectEqual(1, affected);
        } else |err| {
            try t.fail(c, err);
        }
    }

    var result = try c.query("select col_json, col_jsonb from all_types where id = $1", .{4});
    defer result.deinit();

    const row = (try result.next()) orelse unreachable;
    try t.expectString("{\"id\":1,\"name\":\"Leto\"}", row.get([]u8, 0));
    try t.expectString("{\"id\": 2, \"name\": \"Ghanima\"}", row.get(?[]const u8, 1).?);
}

test "Conn: prepare" {
    var c = t.connect(.{});
    defer c.deinit();

    var stmt = try c.prepare("select $1::int where $2");
    try stmt.bind(938);
    try stmt.bind(true);

    var result = try stmt.execute();
    defer result.deinit();

    var row = (try result.next()) orelse unreachable;
    try t.expectEqual(938, row.get(i32, 0));

    try t.expectEqual(null, try result.next());
}

test "PG: row" {
    var c = t.connect(.{});
    defer c.deinit();

    const r1 = try c.row("select 1 where $1", .{false});
    try t.expectEqual(null, r1);

    var r2 = (try c.row("select 2 where $1", .{true})) orelse unreachable;
    try t.expectEqual(2, r2.get(i32, 0));
    try r2.deinit();

    // make sure the conn is still valid after a successful row
    var r3 = (try c.row("select $1::int where $2", .{ 3, true })) orelse unreachable;
    try t.expectEqual(3, r3.get(i32, 0));
    try r3.deinit();

    // make sure the conn is still valid after MoreThanOneRow error
    var r4 = (try c.row("select $1::text where $2", .{ "hi", true })) orelse unreachable;
    try t.expectString("hi", r4.get([]u8, 0));
    try r4.deinit();
}

test "PG: begin/commit" {
    var c = t.connect(.{});
    defer c.deinit();

    try c.begin();
    _ = try c.exec("delete from simple_table", .{});
    _ = try c.exec("insert into simple_table values ($1)", .{"begin_commit"});
    try c.commit();

    var row = (try c.row("select value from simple_table", .{})).?;
    defer row.deinit() catch {};

    try t.expectString("begin_commit", row.get([]u8, 0));
}

test "PG: begin/rollback" {
    var c = t.connect(.{});
    defer c.deinit();

    _ = try c.exec("delete from simple_table", .{});
    try c.begin();
    _ = try c.exec("insert into simple_table values ($1)", .{"begin_commit"});
    try c.rollback();

    const row = try c.row("select value from simple_table", .{});
    try t.expectEqual(null, row);
}

test "PG: bind enums" {
    var c = t.connect(.{});
    defer c.deinit();

    _ = try c.exec(
        \\ insert into all_types (id, col_enum, col_enum_arr, col_text, col_text_arr)
        \\ values (5, $1, $2, $3, $4)
    , .{ DummyEnum.val1, &[_]DummyEnum{ DummyEnum.val1, DummyEnum.val2 }, DummyEnum.val2, [_]DummyEnum{ DummyEnum.val2, DummyEnum.val1 } });

    var row = (try c.row(
        \\ select col_enum, col_text, col_enum_arr, col_text_arr
        \\ from all_types
        \\ where id = 5
    , .{})) orelse unreachable;
    defer row.deinit() catch {};

    try t.expectString("val1", row.get([]u8, 0));
    try t.expectString("val2", row.get([]u8, 1));

    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    {
        const arr = try row.iterator([]const u8, 2).alloc(aa);
        try t.expectEqual(2, arr.len);
        try t.expectString("val1", arr[0]);
        try t.expectString("val2", arr[1]);
    }

    {
        const arr = try row.iterator([]const u8, 3).alloc(aa);
        try t.expectEqual(2, arr.len);
        try t.expectString("val2", arr[0]);
        try t.expectString("val1", arr[1]);
    }
}

test "PG: numeric" {
    defer t.reset();

    var c = t.connect(.{});
    defer c.deinit();

    {
        // read
        var row = (try c.row(
            \\ select 'nan'::numeric, '+Inf'::numeric, '-Inf'::numeric,
            \\ 0::numeric, 0.0::numeric, -0.00009::numeric, -999999.888880::numeric,
            \\ 0.000008, 999999.888807::numeric, 123456.78901234::numeric(14, 8)
        , .{})).?;
        defer row.deinit() catch {};

        try t.expectEqual(true, std.math.isNan(row.get(f64, 0)));
        try t.expectEqual(true, std.math.isInf(row.get(f64, 1)));
        try t.expectEqual(true, std.math.isNegativeInf(row.get(f64, 2)));
        try t.expectEqual(0, row.get(f64, 3));
        try t.expectEqual(0, row.get(f64, 4));
        try t.expectEqual(-0.00009, row.get(f64, 5));
        try t.expectEqual(-999999.888880, row.get(f64, 6));
        try t.expectEqual(0.000008, row.get(f64, 7));
        try t.expectEqual(999999.888807, row.get(f64, 8));
        try t.expectEqual(123456.78901234, row.get(f64, 9));
    }

    {
        // write + write
        var row = (try c.row(
            \\ select
            \\   $1::numeric, $2::numeric, $3::numeric,
            \\   $4::numeric, $5::numeric, $6::numeric,
            \\   $7::numeric, $8::numeric, $9::numeric,
            \\   $10::numeric, $11::numeric, $12::numeric,
            \\   $13::numeric, $14::numeric, $15::numeric,
            \\   $16::numeric, $17::numeric, $18::numeric,
            \\   $19::numeric, $20::numeric, $21::numeric,
            \\   $22::numeric, $23::numeric, $24::numeric,
            \\   $25::numeric, $26::numeric, $27::numeric[]
        , .{ -0.00089891, 939293122.0001101, "-123.4560991", std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64), std.math.nan(f32), std.math.inf(f32), -std.math.inf(f32), 1.1, 12.98, 123.987, 1234.9876, 12345.98765, 123456.987654, 1234567.9876543, 12345678.98765432, 123456789.987654321, @as(f64, 0), @as(f64, 1), 0, 1, 999999999.9999999, @as(f64, 999999999.9999999), -999999999.9999999, @as(f64, -999999999.9999999), &[_][]const u8{ "1.1", "-0.0034" } })).?;
        defer row.deinit() catch {};

        {
            // test the pg.Numeric fields
            const numeric = row.get(types.Numeric, 1);
            try t.expectEqual(939293122.0001101, numeric.toFloat());
            try t.expectEqual(2, numeric.weight);
            try t.expectEqual(.positive, numeric.sign);
            try t.expectEqual(7, numeric.scale);
            try t.expectSlice(u8, &.{ 0, 9, 15, 89, 12, 50, 0, 1, 3, 242 }, numeric.digits);
        }

        try expectNumeric(row.get(types.Numeric, 0), "-0.00089891");
        try expectNumeric(row.get(types.Numeric, 1), "939293122.0001101");
        try expectNumeric(row.get(types.Numeric, 2), "-123.4560991");

        try expectNumeric(row.get(types.Numeric, 3), "nan");
        try expectNumeric(row.get(types.Numeric, 4), "inf");
        try expectNumeric(row.get(types.Numeric, 5), "-inf");

        try expectNumeric(row.get(types.Numeric, 6), "nan");
        try expectNumeric(row.get(types.Numeric, 7), "inf");
        try expectNumeric(row.get(types.Numeric, 8), "-inf");

        try expectNumeric(row.get(types.Numeric, 9), "1.1");
        try expectNumeric(row.get(types.Numeric, 10), "12.98");
        try expectNumeric(row.get(types.Numeric, 11), "123.987");
        try expectNumeric(row.get(types.Numeric, 12), "1234.9876");
        try expectNumeric(row.get(types.Numeric, 13), "12345.98765");
        try expectNumeric(row.get(types.Numeric, 14), "123456.987654");
        try expectNumeric(row.get(types.Numeric, 15), "1234567.9876543");
        try expectNumeric(row.get(types.Numeric, 16), "12345678.98765432");
        try expectNumeric(row.get(types.Numeric, 17), "123456789.987654321");
        try expectNumeric(row.get(types.Numeric, 18), "0.0");
        try expectNumeric(row.get(types.Numeric, 19), "1.0");
        try expectNumeric(row.get(types.Numeric, 20), "0.0");
        try expectNumeric(row.get(types.Numeric, 21), "1.0");
        try expectNumeric(row.get(types.Numeric, 22), "999999999.9999999");
        try expectNumeric(row.get(types.Numeric, 23), "999999999.9999999");
        try expectNumeric(row.get(types.Numeric, 24), "-999999999.9999999");
        try expectNumeric(row.get(types.Numeric, 25), "-999999999.9999999");

        const arr = try row.iterator(types.Numeric, 26).alloc(t.arena.allocator());
        try t.expectEqual(2, arr.len);
        try t.expectEqual(1.1, arr[0].toFloat());
        try t.expectDelta(-0.0034, arr[1].toFloat(), 0.00000001);
    }
}

const DummyStruct = struct {
    id: i32,
    name: []const u8,
};

const DummyEnum = enum {
    val1,
    val2,
};

test {
    _ = @import("../tests/test_copy.zig");
    _ = @import("../tests/test_cancel_notice.zig");
}

test {
    _ = @import("../tests/test_pipeline.zig");
}
