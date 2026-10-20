import struct

from iggy_lag_exporter import decode_offset, encode_get_group_offset, lag


def test_request_layout_matches_binary_protocol():
    payload = encode_get_group_offset(stream_id=10, topic_id=20, group_id=7, partition_id=5)
    # consumer kind 2 (group), then three numeric identifiers (kind 1, length 4, u32 LE), then flag + partition
    assert payload == (
        bytes([2, 1, 4]) + struct.pack("<I", 7)
        + bytes([1, 4]) + struct.pack("<I", 10)
        + bytes([1, 4]) + struct.pack("<I", 20)
        + bytes([1]) + struct.pack("<I", 5)
    )


def test_decode_offset_response():
    assert decode_offset(b"") is None
    assert decode_offset(struct.pack("<IQQ", 3, 1000, 940)) == (3, 1000, 940)


def test_lag():
    assert lag(current_offset=0, messages_count=0, stored_offset=None) == 0
    assert lag(current_offset=99, messages_count=100, stored_offset=None) == 100
    assert lag(current_offset=99, messages_count=100, stored_offset=99) == 0
    assert lag(current_offset=99, messages_count=100, stored_offset=59) == 40
