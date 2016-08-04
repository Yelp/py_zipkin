import mock

from python_zipkin import thread_local

# Can't patch an attribute that doesn't yet exist
thread_local._thread_local.requests = []


@mock.patch('python_zipkin.thread_local._thread_local.requests', ['foo'])
def test_get_thread_local_requests_returns_back_request_if_present():
    assert thread_local.get_thread_local_requests() == ['foo']


def test_get_thread_local_requests_creates_empty_list_if_not_attached():
    delattr(thread_local._thread_local, "requests")
    assert not hasattr(thread_local._thread_local, "requests")
    assert thread_local.get_thread_local_requests() == []
    assert hasattr(thread_local._thread_local, "requests")


@mock.patch('python_zipkin.thread_local._thread_local.requests', [])
def test_get_zipkin_attrs_returns_none_if_no_requests():
    assert not thread_local.get_zipkin_attrs()


@mock.patch('python_zipkin.thread_local._thread_local.requests', ['foo'])
def test_get_zipkin_attrs_returns_the_last_of_the_list():
    assert 'foo' == thread_local.get_zipkin_attrs()


@mock.patch('python_zipkin.thread_local._thread_local.requests', [])
def test_pop_zipkin_attrs_does_nothing_if_no_requests():
    assert not thread_local.pop_zipkin_attrs()


@mock.patch('python_zipkin.thread_local._thread_local.requests', ['foo', 'bar'])
def test_pop_zipkin_attrs_removes_the_last_request():
    assert 'bar' == thread_local.pop_zipkin_attrs()
    assert 'foo' == thread_local.get_zipkin_attrs()


@mock.patch('python_zipkin.thread_local._thread_local.requests', ['foo'])
def test_push_zipkin_attrs_adds_new_request_to_list():
    assert 'foo' == thread_local.get_zipkin_attrs()
    thread_local.push_zipkin_attrs('bar')
    assert 'bar' == thread_local.get_zipkin_attrs()
