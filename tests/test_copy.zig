const std = @import("std");
const lib = @import("../src/lib.zig");
const Conn = lib.Conn;
const t = lib.testing;

test "PG: copy in" {
    var c = t.connect(.{});
    defer c.deinit();

    _ = try c.exec("create temp table copy_test (id int, name text)", .{});

    var sess = try c.copy("copy copy_test from stdin");
    defer sess.deinit();

    try sess.write("1\tleto\n");
    try sess.write("2\tghanima\n");
    try sess.done();

    const row1 = (try c.row("select id, name from copy_test where id = 1", .{})).?;
    defer row1.deinit() catch {};
    try t.expectEqual(1, row1.get(i32, 0));
    try t.expectString("leto", row1.get([]u8, 1));

    const row2 = (try c.row("select id, name from copy_test where id = 2", .{})).?;
    defer row2.deinit() catch {};
    try t.expectEqual(2, row2.get(i32, 0));
    try t.expectString("ghanima", row2.get([]u8, 1));
}

test "PG: copy out" {
    var c = t.connect(.{});
    defer c.deinit();

    _ = try c.exec("create temp table copy_test (id int, name text)", .{});
    _ = try c.exec("insert into copy_test values (1, 'leto'), (2, 'ghanima')", .{});

    var sess = try c.copy("copy copy_test to stdout");
    defer sess.deinit();

    const data1 = (try sess.next()).?;
    try t.expectString("1\tleto\n", data1);

    const data2 = (try sess.next()).?;
    try t.expectString("2\tghanima\n", data2);

    try t.expectEqual(null, try sess.next());
}

test "PG: copy fail" {
    var c = t.connect(.{});
    defer c.deinit();

    _ = try c.exec("create temp table copy_test (id int, name text)", .{});

    var sess = try c.copy("copy copy_test from stdin");
    defer sess.deinit();

    try sess.write("1\tleto\n");
    try sess.fail("something bad happened");

    // The transaction should be aborted or the table should be empty/partial depending on PG version/config,
    // but typically a CopyFail aborts the copy command.
    // However, verify that the connection is still usable.

    const row = try c.row("select count(*) from copy_test", .{});
    try t.expectEqual(0, row.?.get(i64, 0));
}
