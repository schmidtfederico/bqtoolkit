import pytest

from bqtoolkit import BQTable


def test_empty_init_fails():
    with pytest.raises(TypeError):
        BQTable()
