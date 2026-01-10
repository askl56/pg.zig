const std = @import("std");
const proto = @import("_proto.zig");
const Allocator = std.mem.Allocator;

const CopyInResponse = @This();

format: u8,
columns: u16,
column_formats: []u16,

pub fn parse(allocator: Allocator, data: []const u8) !CopyInResponse {
    var reader = proto.Reader.init(data);
    const format = try reader.byte();
    const columns = try reader.int16();
    var column_formats = try allocator.alloc(u16, columns);

    for (0..columns) |i| {
        column_formats[i] = try reader.int16();
    }

    return CopyInResponse{
        .format = format,
        .columns = columns,
        .column_formats = column_formats,
    };
}

pub fn deinit(self: CopyInResponse, allocator: Allocator) void {
    allocator.free(self.column_formats);
}

const t = proto.testing;
test "CopyInResponse: parse" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    // format: 0 (text)
    try buf.writeByte(0);
    // columns: 2
    try buf.writeIntBig(u16, 2);
    // column formats: 0, 0
    try buf.writeIntBig(u16, 0);
    try buf.writeIntBig(u16, 0);

    const msg = try CopyInResponse.parse(t.allocator, buf.string());
    defer msg.deinit(t.allocator);

    try t.expectEqual(0, msg.format);
    try t.expectEqual(2, msg.columns);
    try t.expectEqual(2, msg.column_formats.len);
    try t.expectEqual(0, msg.column_formats[0]);
    try t.expectEqual(0, msg.column_formats[1]);
}
