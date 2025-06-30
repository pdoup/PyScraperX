import asyncio
import logging
from collections import defaultdict
from typing import Any, Dict, List, Set

from fastapi import WebSocket
from fastapi.websockets import WebSocketState
import orjson

from models import WSTopic

logger = logging.getLogger("WebScraper")


class WSConnectionManager:
    """
    Manages active WebSocket connections and topic-based subscriptions.
    """

    def __init__(self):
        """Initializes the ConnectionManager."""
        self.subscriptions: Dict[WSTopic, List[WebSocket]] = defaultdict(list)
        self.client_topics: Dict[WebSocket, Set[WSTopic]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        """
        Accepts a new WebSocket connection. The client must then send a
        'subscribe' message to start receiving topic updates.
        """
        await websocket.accept()
        logger.info(
            f"Client connected: {websocket.client._asdict()}. Waiting for subscriptions."
        )

    async def disconnect(self, websocket: WebSocket):
        """
        Removes a WebSocket connection and all its subscriptions.
        """
        async with self._lock:
            if websocket in self.client_topics:
                topics_to_remove_from = self.client_topics.pop(websocket)
                for topic in topics_to_remove_from:
                    if websocket in self.subscriptions.get(topic, []):
                        self.subscriptions[topic].remove(websocket)
        logger.info(
            f"Client disconnected: {websocket.client!s}. Cleaned up subscriptions."
        )

    async def subscribe(self, websocket: WebSocket, topic: WSTopic):
        """
        Subscribes a client to a given topic.
        """
        async with self._lock:
            self.subscriptions[topic].append(websocket)
            self.client_topics[websocket].add(topic)
        logger.info(
            f"Client {websocket.client._asdict()} subscribed to topic '{topic.value}'."
        )

    async def unsubscribe(self, websocket: WebSocket, topic: WSTopic):
        """
        Unsubscribes a client from a given topic.
        """
        async with self._lock:
            if topic in self.subscriptions and websocket in self.subscriptions[topic]:
                self.subscriptions[topic].remove(websocket)
            if (
                websocket in self.client_topics
                and topic in self.client_topics[websocket]
            ):
                self.client_topics[websocket].remove(topic)
        logger.info(
            f"Client {websocket.client._asdict()} unsubscribed from topic '{topic.value}'."
        )

    async def broadcast_to_topic(self, topic: WSTopic, data: Any):
        """
        Broadcasts a JSON-serializable message to all clients subscribed to a specific topic.
        """
        if not self.subscriptions[topic]:
            return

        async with self._lock:
            connections_to_send = self.subscriptions[topic][:]

        if not connections_to_send:
            return

        logger.debug(
            f"Broadcasting to {len(connections_to_send)} clients on topic '{topic.value}'."
        )

        results = await asyncio.gather(
            *(connection.send_json(data) for connection in connections_to_send),
            return_exceptions=True,
        )

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                disconnected_ws = connections_to_send[i]
                logger.warning(
                    f"Failed to send message to client on topic '{topic.value}', assuming disconnection: {result}"
                )
                await self.disconnect(disconnected_ws)

    async def broadcast_to_topic_binary(self, topic: WSTopic, data: Any):
        """
        Broadcasts a message to all clients subscribed to a specific topic
        using an optimized and compressed binary format.
        """
        async with self._lock:
            if not self.subscriptions[topic]:
                return
            connections_to_send = self.subscriptions[topic][:]

        if not connections_to_send:
            return

        logger.debug(
            f"Broadcasting to {len(connections_to_send)} clients on topic '{topic.value}'."
        )

        bin_payload: bytes = orjson.dumps(data)

        results = await asyncio.gather(
            *[
                self._send_to_connection(conn, bin_payload)
                for conn in connections_to_send
            ],
            return_exceptions=True,
        )

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                disconnected_ws = connections_to_send[i]
                logger.warning(
                    f"Failed to send to client on topic '{topic.value}', initiating disconnection: {result}"
                )
                await self.disconnect(disconnected_ws)

    async def _send_to_connection(self, websocket: WebSocket, data: bytes):
        """
        A robust wrapper for sending data to a single websocket connection.
        """
        if websocket.client_state == WebSocketState.CONNECTED:
            await websocket.send_bytes(data)
