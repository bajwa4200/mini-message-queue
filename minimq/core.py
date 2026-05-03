from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable

@dataclass
class Message:
    topic: str
    payload: Any

class MiniMQ:
    def __init__(self) -> None:
        self._queues: dict[str, deque[Message]] = defaultdict(deque)
        self._subs: dict[str, list[Callable[[Message], None]]] = defaultdict(list)

    def subscribe(self, topic: str, handler: Callable[[Message], None]) -> None:
        self._subs[topic].append(handler)

    def publish(self, topic: str, payload: Any) -> int:
        msg = Message(topic, payload)
        self._queues[topic].append(msg)
        delivered = 0
        for h in self._subs[topic]:
            h(msg)
            delivered += 1
        return delivered

    def poll(self, topic: str) -> Message | None:
        q = self._queues[topic]
        return q.popleft() if q else None

    def pending(self, topic: str) -> int:
        return len(self._queues[topic])
