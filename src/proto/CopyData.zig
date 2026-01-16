const std = @import("std");
const proto = @import("_proto.zig");

const CopyData = @This();

data: []const u8,

pub fn write(self: CopyData, buf: *proto.Buffer) !void {
    // 4 + N
    // len + data
    const payload_len = 4 + self.data.len;
    const total_length = payload_len + 1;

    try buf.ensureTotalCapacity(total_length);

    var view = buf.skip(total_length) catch unreachable;
    view.writeByte('d');
    view.writeIntBig(u32, @intCast(payload_len));
    view.write(self.data);
}

pub fn parse(data: []const u8) CopyData {
    return .{ .data = data };
}

const t = proto.testing;
const Reader = proto.Reader;
test "CopyData: write" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    const cd = CopyData{ .data = "abc" };
    try cd.write(&buf);

    var reader = Reader.init(buf.string());
    try t.expectEqual('d', try reader.byte());
    try t.expectEqual(7, try reader.int32()); // payload length
    try t.expectString("abc", reader.rest());
}
