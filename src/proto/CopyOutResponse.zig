const std = @import("std");
const proto = @import("_proto.zig");
const Allocator = std.mem.Allocator;

const CopyOutResponse = @This();

format: u8,
columns: u16,
column_formats: []u16,

pub fn parse(allocator: Allocator, data: []const u8) !CopyOutResponse {
    var reader = proto.Reader.init(data);
    const format = try reader.byte();
    const columns = try reader.int16();
    var column_formats = try allocator.alloc(u16, columns);

    for (0..columns) |i| {
        column_formats[i] = try reader.int16();
    }

    return CopyOutResponse{
        .format = format,
        .columns = columns,
        .column_formats = column_formats,
    };
}

pub fn deinit(self: CopyOutResponse, allocator: Allocator) void {
    allocator.free(self.column_formats);
}

const t = proto.testing;
test "CopyOutResponse: parse" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    // format: 1 (binary)
    try buf.writeByte(1);
    // columns: 1
    try buf.writeIntBig(u16, 1);
    // column formats: 1
    try buf.writeIntBig(u16, 1);

    const msg = try CopyOutResponse.parse(t.allocator, buf.string());
    defer msg.deinit(t.allocator);

    try t.expectEqual(1, msg.format);
    try t.expectEqual(1, msg.columns);
    try t.expectEqual(1, msg.column_formats.len);
    try t.expectEqual(1, msg.column_formats[0]);
}
