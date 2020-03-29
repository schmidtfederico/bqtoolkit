from bqtoolkit.utils import format_bytes


def test_format_bytes():
    assert format_bytes(0) == '0 bytes'
    assert format_bytes(1023) == '1023 bytes'
    assert format_bytes(1024) == '1.00 KB'
    assert format_bytes(1024 * 1024 - 1).endswith('KB')
    assert format_bytes(1024 * 1024).endswith('MB')
    assert format_bytes(1024 ** 3).endswith('GB')
    assert format_bytes(1024 ** 4).endswith('TB')
