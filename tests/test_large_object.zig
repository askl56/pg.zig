const std = @import("std");
const lib = @import("../src/lib.zig");
const t = lib.testing;
const LargeObject = lib.LargeObject;

test "PG: large object" {
    var c = t.connect(.{});
    defer c.deinit();

    // Create
    // In Fast Path, we should probably be in a transaction for LO operations?
    // "Large objects must be used inside a transaction block."
    try c.begin();

    const oid = try LargeObject.create(&c, LargeObject.INV_READ | LargeObject.INV_WRITE);

    // Open
    var lo = try LargeObject.open(&c, oid, LargeObject.INV_WRITE);

    // Write
    const data = "Hello Large Object";
    const written = try lo.write(data);
    try t.expectEqual(data.len, written);

    try lo.close();

    // Re-open for read
    var lo2 = try LargeObject.open(&c, oid, LargeObject.INV_READ);

    // Read
    const read_data = try lo2.read(100);
    defer c._allocator.free(read_data);

    try t.expectString(data, read_data);

    // Seek
    const pos = try lo2.seek(6, LargeObject.SEEK_SET);
    try t.expectEqual(6, pos);

    const partial_read = try lo2.read(100);
    defer c._allocator.free(partial_read);
    try t.expectString("Large Object", partial_read);

    try lo2.close();

    // Unlink
    try LargeObject.unlink(&c, oid);

    try c.commit();
}
