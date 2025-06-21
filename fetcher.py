import json
import logging
from asyncio.exceptions import TimeoutError
from typing import Dict, List, Optional

import aiohttp
from aiohttp import ClientError, ClientResponseError, ClientTimeout
from pydantic import HttpUrl, ValidationError

from models import RecordModel

logger = logging.getLogger("WebScraper")


class DataFetcher:
    def __init__(self, endpoint: HttpUrl, session: aiohttp.ClientSession):
        self.endpoint = endpoint
        self.session = session

    async def fetch(self) -> List[Dict]:
        """
        Fetches data from the configured endpoint using the provided shared session.
        Validates the data and returns a list of validated records.
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
                timeout=ClientTimeout(total=10),
            ) as response:
                response.raise_for_status()

                content_type = response.headers.get("Content-Type", "")
                if "application/json" not in content_type:
                    logger.error(
                        f"Fetching error from {self.endpoint}: Expected JSON but got "
                        f"unexpected mimetype: {content_type}. Status: {response.status}. "
                        f"Response text start: {await response.text()[:200]}..."
                    )
                    raise TypeError("Invalid mime type detected")

                raw_data = await response.json()

                validated_data = self._validate_single_record(raw_data)

                return [validated_data] if validated_data else []

        except ClientResponseError as e:
            logger.error(f"HTTP error fetching {self.endpoint}: {e}")
            raise e
        except ClientError as e:
            logger.error(f"Network or HTTP error fetching {self.endpoint}: {e}")
            raise e
        except TimeoutError as e:
            logger.error(f"Timeout error reached fetching {self.endpoint}: {e}")
            raise e
        except json.JSONDecodeError as e:
            logger.error(
                f"JSON decoding error from {self.endpoint}: {e}. Response might not be JSON."
            )
            raise e
        except ValidationError as e:
            logger.error(f"Data validation error for {self.endpoint}: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while fetching {self.endpoint}: {e}",
                exc_info=True,
            )
            raise e

    def _validate_single_record(self, raw_data: Dict) -> Optional[Dict]:
        try:
            record = RecordModel.from_dict(raw_data)
            validated_record = record.model_dump()
            logger.info(f"Validated 1 record from {self.endpoint}.")
            return validated_record
        except ValidationError as e:
            logger.warning(
                f"Validation failed for record from {self.endpoint}: {e}. Raw data: {raw_data}"
            )
            return None
        except TypeError as e:
            logger.warning(
                f"Type error during validation from {self.endpoint}: {e}. Raw data: {raw_data}"
            )
            return None
        except Exception as e:
            logger.warning(
                f"Unexpected error during validation from {self.endpoint}: {e}. Raw data: {raw_data}"
            )
            return None
