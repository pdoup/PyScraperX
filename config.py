import logging
from collections import namedtuple
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field, HttpUrl, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("WebScraper")


class Config(BaseModel):
    urls: List[HttpUrl]


ResolveTime = namedtuple("ResolveTime", ("interval", "unit"), defaults=(30, "seconds"))


def load_urls(filepath: Path) -> Config:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            lines = list(line.strip() for line in f if line.strip())
            logger.debug(lines)
        return Config(urls=lines)
    except ValidationError as e:
        logger.error(f"Invalid URLs in config: {e}")
        raise
    except FileNotFoundError:
        logger.error(f"{filepath.resolve(strict=False)} not found")
        raise


class AppSettings(BaseSettings):
    """
    Represents the application's configuration settings.

    Settings are loaded from environment variables or a .env file.
    Pydantic-settings automatically matches environment variables (case-insensitive)
    to the field names defined in this class.
    """

    run_every_seconds: Optional[int] = Field(
        default=None,
        gt=0,
        description="The interval in seconds at which the primary task should run.",
    )

    run_every_minutes: Optional[int] = Field(
        default=None,
        gt=0,
        description="The interval in minutes at which a secondary task should run.",
    )

    max_retries: int = Field(
        default=3,
        gt=0,
        description="The maximum number of times to retry a failed job.",
    )

    urls_path: Path = Field(..., description="Path to the URLs file", frozen=True)

    update_interval_ms: int = Field(
        default=1000, description="Update UI every X ms", ge=1000
    )

    @property
    def run_interval(self) -> ResolveTime:
        """
        Determines the effective run interval and its unit ('seconds' or 'minutes').

        This property encapsulates the scheduling logic:
        1. It prioritizes `run_every_seconds` if that value is set.
        2. If not, it falls back to `run_every_minutes`.
        3. If neither is set, it defaults to a 30-second interval.

        Returns:
            A tuple containing the interval (int) and the unit (str),
            e.g., (30, "seconds").
        """
        return (
            ResolveTime(self.run_every_seconds, "seconds")
            if self.run_every_seconds is not None
            else (
                ResolveTime(self.run_every_minutes, "minutes")
                if self.run_every_minutes is not None
                else ResolveTime()
            )
        )

    @property
    def interval_in_seconds(self) -> int:
        """
        Calculates the effective run interval in seconds.

        The logic is as follows:
        1. If `run_every_seconds` is set, its value is used directly.
        2. If not, but `run_every_minutes` is set, its value is converted to seconds.

        Returns:
            The final run interval in seconds (int).
        """
        if rt := self.run_interval:
            return rt.interval * 60 if rt.unit == "minutes" else rt.interval
        else:
            logger.warning("Failed to resolve time in seconds, defaulting to 30")
            return ResolveTime().interval

    model_config = SettingsConfigDict(
        env_file=".env.local",
        env_file_encoding="utf-8",
        extra="forbid",
        frozen=True,
    )


settings: AppSettings = AppSettings()
