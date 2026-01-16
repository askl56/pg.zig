const std = @import("std");
const proto = @import("_proto.zig");

const CopyFail = @This();

message: []const u8,

pub fn write(self: CopyFail, buf: *proto.Buffer) !void {
    // 4 + N + 1
    // len + message + 0
    const payload_len = 4 + self.message.len + 1;
    const total_length = payload_len + 1;

    try buf.ensureTotalCapacity(total_length);

    var view = buf.skip(total_length) catch unreachable;
    view.writeByte('f');
    view.writeIntBig(u32, @intCast(payload_len));
    view.write(self.message);
    view.writeByte(0);
}

const t = proto.testing;
const Reader = proto.Reader;
test "CopyFail: write" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    const cf = CopyFail{ .message = "fail" };
    try cf.write(&buf);

    var reader = Reader.init(buf.string());
    try t.expectEqual('f', try reader.byte());
    try t.expectEqual(9, try reader.int32()); // payload length
    try t.expectString("fail", try reader.string());
}
