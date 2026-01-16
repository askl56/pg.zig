const std = @import("std");
const lib = @import("../src/lib.zig");
const t = lib.testing;

test "PG: pipeline" {
    var c = t.connect(.{});
    defer c.deinit();

    _ = try c.exec("create temp table pipe_test (id int)", .{});

    var p = try c.pipeline();

    // Queue 3 inserts and 1 select
    try p.query("insert into pipe_test values ($1)", .{1});
    try p.query("insert into pipe_test values ($2)", .{2});
    try p.query("insert into pipe_test values ($1)", .{3});
    try p.query("select id from pipe_test order by id", .{});

    try p.sync();

    var it = p.results();

    // Result 1: Insert
    {
        const r = (try it.next()).?;
        defer r.deinit();
        try t.expectEqual(null, try r.next()); // Consume CommandComplete
        try t.expectEqual(1, r.rows_affected);
    }

    // Result 2: Insert
    {
        const r = (try it.next()).?;
        defer r.deinit();
        try t.expectEqual(null, try r.next());
        try t.expectEqual(1, r.rows_affected);
    }

    // Result 3: Insert
    {
        const r = (try it.next()).?;
        defer r.deinit();
        try t.expectEqual(null, try r.next());
        try t.expectEqual(1, r.rows_affected);
    }

    // Result 4: Select
    {
        const r = (try it.next()).?;
        defer r.deinit();

        const row1 = (try r.next()).?;
        try t.expectEqual(1, row1.get(i32, 0));

        const row2 = (try r.next()).?;
        try t.expectEqual(2, row2.get(i32, 0));

        const row3 = (try r.next()).?;
        try t.expectEqual(3, row3.get(i32, 0));

        try t.expectEqual(null, try r.next());
    }

    // End of pipeline
    try t.expectEqual(null, try it.next());
}
