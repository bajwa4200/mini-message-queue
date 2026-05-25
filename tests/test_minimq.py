from minimq.core import MiniMQ

def test_publish_poll():
    mq = MiniMQ()
    mq.publish("news", {"x": 1})
    m = mq.poll("news")
    assert m.payload["x"] == 1

def test_subscriber_called():
    mq = MiniMQ()
    seen = []
    mq.subscribe("t", lambda m: seen.append(m.payload))
    mq.publish("t", 42)
    assert seen == [42]

def test_pending():
    mq = MiniMQ()
    mq.publish("a", 1)
    mq.publish("a", 2)
    assert mq.pending("a") == 2

def test_empty_poll():
    assert MiniMQ().poll("none") is None

def test_delivery_count():
    mq = MiniMQ()
    mq.subscribe("x", lambda m: None)
    mq.subscribe("x", lambda m: None)
    assert mq.publish("x", 0) == 2
