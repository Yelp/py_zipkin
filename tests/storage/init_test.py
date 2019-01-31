import mock
import pytest

from py_zipkin import storage


def test_default_span_storage_warns():
    with mock.patch.object(storage.log, 'warning') as mock_log:
        storage.default_span_storage()
        assert mock_log.call_count == 1


class TestLocalStorage(object):
    def test_storage_raises_if_not_implemented(self):
        with pytest.raises(NotImplementedError):
            storage.LocalStorage().storage
