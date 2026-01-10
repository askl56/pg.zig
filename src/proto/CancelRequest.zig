const std = @import("std");
const proto = @import("_proto.zig");

const CancelRequest = @This();

process_id: i32,
secret_key: i32,

pub fn write(self: CancelRequest, buf: *proto.Buffer) !void {
    // 4 + 4 + 4 + 4
    // len + 80877102 (magic) + pid + key
    const payload_len = 16;

    try buf.ensureTotalCapacity(payload_len);

    var view = buf.skip(payload_len) catch unreachable;
    view.writeIntBig(u32, 16);
    view.writeIntBig(u32, 80877102);
    view.writeIntBig(u32, @as(u32, @bitCast(self.process_id)));
    view.writeIntBig(u32, @as(u32, @bitCast(self.secret_key)));
}

const t = proto.testing;
const Reader = proto.Reader;
test "CancelRequest: write" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    const cr = CancelRequest{ .process_id = 123, .secret_key = 456 };
    try cr.write(&buf);

    var reader = Reader.init(buf.string());
    try t.expectEqual(16, try reader.int32());
    try t.expectEqual(80877102, try reader.int32());
    try t.expectEqual(123, try reader.int32());
    try t.expectEqual(456, try reader.int32());
}
