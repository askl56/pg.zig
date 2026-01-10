const std = @import("std");
const lib = @import("lib.zig");
const Conn = lib.Conn;

// OIDs for Large Object functions
const LO_CREAT = 957;
const LO_OPEN = 952;
const LO_CLOSE = 953;
const LO_READ = 954;
const LO_WRITE = 955;
const LO_LSEEK = 956;
const LO_UNLINK = 964;

// Modes for lo_open
pub const INV_WRITE: i32 = 0x00020000;
pub const INV_READ: i32 = 0x00040000;

pub const SEEK_SET: i32 = 0;
pub const SEEK_CUR: i32 = 1;
pub const SEEK_END: i32 = 2;

pub const LargeObject = struct {
    conn: *Conn,
    fd: i32,

    pub fn create(conn: *Conn, mode: i32) !i32 {
        const res = try conn.fastCall(LO_CREAT, .{mode});
        if (res) |data| {
            defer conn._allocator.free(data);
            if (data.len < 4) return error.InvalidResponse;
            return std.mem.readInt(i32, data[0..4], .big);
        }
        return error.LargeObjectCreateFailed;
    }

    pub fn open(conn: *Conn, oid: i32, mode: i32) !LargeObject {
        const res = try conn.fastCall(LO_OPEN, .{ oid, mode });
        if (res) |data| {
            defer conn._allocator.free(data);
            if (data.len < 4) return error.InvalidResponse;
            const fd = std.mem.readInt(i32, data[0..4], .big);
            return LargeObject{ .conn = conn, .fd = fd };
        }
        return error.LargeObjectOpenFailed;
    }

    pub fn close(self: *LargeObject) !void {
        const res = try self.conn.fastCall(LO_CLOSE, .{self.fd});
        if (res) |data| {
            self.conn._allocator.free(data);
        }
    }

    pub fn read(self: *LargeObject, len: i32) ![]const u8 {
        const res = try self.conn.fastCall(LO_READ, .{ self.fd, len });
        return res orelse error.LargeObjectReadFailed;
    }

    pub fn write(self: *LargeObject, data: []const u8) !i32 {
        const res = try self.conn.fastCall(LO_WRITE, .{ self.fd, data });
        if (res) |d| {
            defer self.conn._allocator.free(d);
            if (d.len < 4) return error.InvalidResponse;
            return std.mem.readInt(i32, d[0..4], .big);
        }
        return error.LargeObjectWriteFailed;
    }

    pub fn seek(self: *LargeObject, offset: i32, whence: i32) !i32 {
        const res = try self.conn.fastCall(LO_LSEEK, .{ self.fd, offset, whence });
        if (res) |d| {
            defer self.conn._allocator.free(d);
            if (d.len < 4) return error.InvalidResponse;
            return std.mem.readInt(i32, d[0..4], .big);
        }
        return error.LargeObjectSeekFailed;
    }

    pub fn unlink(conn: *Conn, oid: i32) !void {
        const res = try conn.fastCall(LO_UNLINK, .{oid});
        if (res) |data| {
            conn._allocator.free(data);
        }
    }
};
