const std = @import("std");
const proto = @import("_proto.zig");

const FunctionCallResponse = @This();

// 'V'
// Int32 length
// Int32 length of value (-1 if null)
// Byte[n] value

result: ?[]const u8,

pub fn parse(data: []const u8) !FunctionCallResponse {
    var reader = proto.Reader.init(data);
    const len = try reader.int32();
    if (len == -1) {
        return FunctionCallResponse{ .result = null };
    }
    // The rest is the data
    // We need to return a slice.
    // The reader consumes the length.
    return FunctionCallResponse{ .result = reader.rest() };
}

const t = proto.testing;
test "FunctionCallResponse: parse" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    try buf.writeIntBig(i32, 3);
    try buf.write("val");

    const msg = try FunctionCallResponse.parse(buf.string());
    try t.expectString("val", msg.result.?);
}

test "FunctionCallResponse: parse null" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    try buf.writeIntBig(i32, -1);

    const msg = try FunctionCallResponse.parse(buf.string());
    try t.expectEqual(null, msg.result);
}
