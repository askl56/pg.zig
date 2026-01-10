const std = @import("std");
const proto = @import("_proto.zig");

const NoticeResponse = @This();

// Fields are optional because they depend on the error/notice type
code: []const u8 = "",
message: []const u8 = "",
severity: []const u8 = "",
where: ?[]const u8 = null,
detail: ?[]const u8 = null,
hint: ?[]const u8 = null,

pub fn parse(data: []const u8) NoticeResponse {
    var notice = NoticeResponse{};

    var pos: usize = 0;
    while (pos < data.len) {
        const value_end = std.mem.indexOfScalarPos(u8, data, pos + 1, 0) orelse {
            break;
        };

        const value = data[pos + 1 .. value_end];
        switch (data[pos]) {
            'S' => notice.severity = value,
            'C' => notice.code = value,
            'M' => notice.message = value,
            'D' => notice.detail = value,
            'H' => notice.hint = value,
            'W' => notice.where = value,
            else => {}, // Ignore other fields for now
        }
        pos = value_end + 1;
    }

    return notice;
}

const t = proto.testing;
test "NoticeResponse: parse" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    try buf.writeByte('S');
    try buf.write("NOTICE");
    try buf.writeByte(0);

    try buf.writeByte('M');
    try buf.write("This is a notice");
    try buf.writeByte(0);

    const n = NoticeResponse.parse(buf.string());
    try t.expectString("NOTICE", n.severity);
    try t.expectString("This is a notice", n.message);
}
