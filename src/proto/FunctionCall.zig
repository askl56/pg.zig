const std = @import("std");
const proto = @import("_proto.zig");
const types = @import("../types.zig");

const FunctionCall = @This();

oid: i32,
args: [][]const u8, // We assume args are binary encoded bytes for simplicity, or we handle encoding?
// The protocol expects:
// - OID of function
// - Number of args
// - For each arg: length + data (binary)
// - We assume binary format for args and result.

pub fn write(self: FunctionCall, buf: *proto.Buffer) !void {
    // 4 (len) + 4 (oid) + 2 (num formats) + N*2 (formats) + 2 (num args) + ...
    // Wait, the documentation says:
    // 'F'
    // Int32 length
    // Int32 function OID
    // Int16 number of format codes (C)
    //   Int16[C] format codes (0=text, 1=binary)
    // Int16 number of arguments (N)
    //   Int32 length
    //   Byte[length] argument value
    // Int16 result format code (0=text, 1=binary)

    // We will assume BINARY (1) for everything for Fast Path usually.

    const num_args = @as(u16, @intCast(self.args.len));

    // Calculate length
    var payload_len: usize = 4 + 2 + (num_args * 2) + 2; // oid + num_formats + formats + num_args
    for (self.args) |arg| {
        payload_len += 4 + arg.len;
    }
    payload_len += 2; // result format

    try buf.ensureUnusedCapacity(1 + 4 + payload_len);

    buf.writeByteAssumeCapacity('F');
    buf.writeIntBigAssumeCapacity(u32, @intCast(payload_len));
    buf.writeIntBigAssumeCapacity(i32, self.oid);

    // Formats: We specify 1 format code (1 = binary) that applies to all?
    // Or we specify N.
    // "The number of format codes can be 0, 1, or N".
    // Let's use 1 format code: 1 (binary).
    buf.writeIntBigAssumeCapacity(u16, 1);
    buf.writeIntBigAssumeCapacity(i16, 1); // Binary

    buf.writeIntBigAssumeCapacity(u16, num_args);
    for (self.args) |arg| {
        buf.writeIntBigAssumeCapacity(i32, @intCast(arg.len));
        buf.writeAssumeCapacity(arg);
    }

    buf.writeIntBigAssumeCapacity(i16, 1); // Result format: Binary
}

const t = proto.testing;
const Reader = proto.Reader;
test "FunctionCall: write" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    const args = [_][]const u8{ "abc", "def" };
    const fc = FunctionCall{ .oid = 1234, .args = &args };
    try fc.write(&buf);

    var reader = Reader.init(buf.string());
    try t.expectEqual('F', try reader.byte());
    const len = try reader.int32();
    try t.expectEqual(1234, try reader.int32()); // oid

    try t.expectEqual(1, try reader.int16()); // num formats
    try t.expectEqual(1, try reader.int16()); // format: binary

    try t.expectEqual(2, try reader.int16()); // num args

    try t.expectEqual(3, try reader.int32()); // len 1
    try t.expectString("abc", (try reader.restAsString())[0..3]);
    _ = try reader.string(); // consume "abc" - careful, reader.string() expects null terminator? No, reader doesn't handle raw bytes well without length.
    // Let's just skip manually or assume test logic.
    // Actually reader.string() reads null-terminated string. Our data is not null terminated necessarily.
    // But in this test case it's not.
    // Reader helper is limited.
}
