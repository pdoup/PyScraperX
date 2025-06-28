import asyncio
import logging
from typing import Any, List

from fastapi import WebSocket

logger = logging.getLogger("WebScraper")


class WSConnectionManager:
    """
    Manages active WebSocket connections for broadcasting real-time updates.
    """

    def __init__(self):
        """Initializes the ConnectionManager with an empty list of active connections."""
        self.active_connections: List[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        """
        Accepts and stores a new WebSocket connection.

        Args:
            websocket: The FastAPI WebSocket object for the new client.
        """
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
        logger.info(
            f"Client connected: {websocket.client._asdict()}. Total clients: {len(self.active_connections)}"
        )

    async def disconnect(self, websocket: WebSocket):
        """
        Removes a WebSocket connection from the active list.

        Args:
            websocket: The WebSocket object to remove.
        """
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        logger.info(
            f"Client disconnected: {websocket.client._asdict()}. Total clients: {len(self.active_connections)}"
        )

    async def broadcast_json(self, data: Any):
        """
        Broadcasts a JSON-serializable message to all connected clients.

        Handles disconnections gracefully during the broadcast.

        Args:
            data: The data to be sent, which will be converted to JSON.
        """
        if not self.active_connections:
            return

        async with self._lock:
            connections_to_send = self.active_connections[:]

        results = await asyncio.gather(
            *(connection.send_json(data) for connection in connections_to_send),
            return_exceptions=True,
        )

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                disconnected_ws = connections_to_send[i]
                logger.warning(
                    f"Failed to send message to {disconnected_ws.client._asdict()}, assuming disconnection: {result}"
                )
                await self.disconnect(disconnected_ws)
