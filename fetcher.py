import asyncio
import json
import logging
from asyncio.exceptions import TimeoutError
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import ClientError, ClientResponseError, ClientTimeout
from pydantic import HttpUrl, TypeAdapter, ValidationError

from models import RecordModel

logger = logging.getLogger("WebScraper")


class DataFetcher:
    """
    A robust class to fetch and validate data from a JSON endpoint.
    It is designed to gracefully handle endpoints that return either a single JSON object or a JSON array (list) of objects.
    """

    def __init__(self, endpoint: HttpUrl, session: aiohttp.ClientSession):
        self.endpoint = endpoint
        self.session = session
        self.record_list_validator = TypeAdapter(List[RecordModel])

    async def fetch(self) -> List[Dict]:
        """
        Fetches data from the configured endpoint.

        The endpoint can return either a single JSON object or a JSON array of objects.
        The data is validated against the RecordModel, and a list of successfully validated records is returned.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }

        try:
            async with self.session.get(
                self.endpoint.unicode_string(),
                headers=headers,
                allow_redirects=False,
                timeout=ClientTimeout(total=15),
            ) as response:
                response.raise_for_status()

                content_type = response.headers.get("Content-Type", "")
                if "application/json" not in content_type:
                    logger.error(
                        f"Fetching error from {self.endpoint}: Expected JSON but got "
                        f"unexpected mimetype: {content_type}. Status: {response.status}. "
                        f"Response text start: {await response.text(encoding='utf-8', errors='ignore')[:200]}..."
                    )
                    return []

                raw_data: Any = await response.json()

                records_to_process: List[Dict]
                if isinstance(raw_data, list):
                    records_to_process = raw_data
                elif isinstance(raw_data, dict):
                    records_to_process = [raw_data]
                else:
                    logger.error(
                        f"Data format error from {self.endpoint}: Expected a JSON object or array, "
                        f"but received type {type(raw_data).__name__}."
                    )
                    return []
                return await self._validate_records_async(records_to_process)

        except ClientResponseError as e:
            logger.error(f"HTTP error fetching {self.endpoint}: {e}")
            raise
        except ClientError as e:
            logger.error(f"Network or HTTP error fetching {self.endpoint}: {e}")
            raise
        except TimeoutError:
            logger.error(f"Timeout error reached fetching {self.endpoint}")
            raise
        except json.JSONDecodeError:
            response_text = await response.text(encoding="utf-8", errors="ignore")
            logger.error(
                f"JSON decoding error from {self.endpoint}. Response text: {response_text[:500]}..."
            )
            raise
        except ValidationError as e:
            logger.error(f"Data validation error for batch from {self.endpoint}: {e}")
            raise
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while fetching {self.endpoint}: {e}",
                exc_info=True,
            )
            raise

    def _validate_records_sync(self, raw_data_list: List[Dict]) -> List[Dict]:
        """
        Validates a list of raw records using the Pydantic TypeAdapter.

        Args:
            raw_data_list: A list of dictionaries, where each is a potential record.

        Returns:
            A list of validated records as dictionaries. Invalid records are excluded.
        """
        if not raw_data_list:
            return []

        try:
            validated_models: List = self.record_list_validator.validate_python(
                raw_data_list, from_attributes=True
            )
            validated_dicts = [record.model_dump() for record in validated_models]

            logger.info(
                f"Successfully validated {len(validated_dicts)}/{len(raw_data_list)} "
                f"records from {self.endpoint}."
            )
            return validated_dicts
        except ValidationError as e:
            logger.warning(
                f"Validation failed for one or more records from {self.endpoint}. "
                f"Error: {e}"
            )
            return []
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during sync validation: {e}",
                exc_info=True,
            )
            return []

    async def _validate_records_async(self, raw_data_list: List[Dict]) -> List[Dict]:
        """
        Asynchronous wrapper that runs the synchronous validation
        in a separate thread to avoid blocking the event loop.
        """
        return await asyncio.to_thread(self._validate_records_sync, raw_data_list)
