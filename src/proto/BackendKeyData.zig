const std = @import("std");
const proto = @import("_proto.zig");

const BackendKeyData = @This();

process_id: i32,
secret_key: i32,

pub fn parse(data: []const u8) !BackendKeyData {
    var reader = proto.Reader.init(data);
    const pid = try reader.int32();
    const key = try reader.int32();
    return BackendKeyData{
        .process_id = @as(i32, @bitCast(pid)),
        .secret_key = @as(i32, @bitCast(key)),
    };
}

const t = proto.testing;
test "BackendKeyData: parse" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    try buf.writeIntBig(u32, 1234);
    try buf.writeIntBig(u32, 5678);

    const msg = try BackendKeyData.parse(buf.string());
    try t.expectEqual(1234, msg.process_id);
    try t.expectEqual(5678, msg.secret_key);
}
