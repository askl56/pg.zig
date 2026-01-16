const std = @import("std");
const proto = @import("_proto.zig");

const CopyDone = @This();

pub fn write(buf: *proto.Buffer) !void {
    // 4 + 1
    // len + type
    const payload_len = 4;
    const total_length = payload_len + 1;

    try buf.ensureTotalCapacity(total_length);

    var view = buf.skip(total_length) catch unreachable;
    view.writeByte('c');
    view.writeIntBig(u32, @intCast(payload_len));
}

const t = proto.testing;
const Reader = proto.Reader;
test "CopyDone: write" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    try CopyDone.write(&buf);

    var reader = Reader.init(buf.string());
    try t.expectEqual('c', try reader.byte());
    try t.expectEqual(4, try reader.int32()); // payload length
}
