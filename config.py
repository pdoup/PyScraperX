import logging
from collections import namedtuple
from pathlib import Path
from typing import Annotated, Any, List, Optional

from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    HttpUrl,
    IPvAnyAddress,
    ValidationError,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("WebScraper")


def validate_log_level(v: Any) -> str:
    """Validates if the provided value is a valid Uvicorn log level."""
    valid_levels = ("critical", "error", "warning", "info", "debug", "trace")
    if isinstance(v, str) and v.lower() in valid_levels:
        return v.lower()
    raise ValueError(f"Invalid log level: '{v}'. Must be one of {valid_levels}")


LogLevelType = Annotated[str, BeforeValidator(validate_log_level)]


class UvicornServerSettings(BaseSettings):
    """
    Settings for the Uvicorn web server.
    """

    model_config = SettingsConfigDict(
        extra="allow",
        env_file=".env.server",
        env_prefix="SERVER_",
        env_ignore_empty=True,
        env_parse_none_str=True,
    )

    host: Annotated[
        IPvAnyAddress,
        Field(
            default="127.0.0.1",
            description="Host to bind the server to.",
            validate_default=False,
            frozen=True,
        ),
    ]
    port: int = Field(
        default=8000,
        gt=0,
        le=65535,
        description="Port to bind the server to. Must be between 1 and 65535.",
        validate_default=False,
        frozen=True,
    )
    log_level: Optional[LogLevelType] = Field(
        default="info",
        description="Log level for Uvicorn (e.g., 'info', 'warning', 'debug', 'trace').",
        validate_default=False,
    )
    timeout_graceful_shutdown: Optional[int] = Field(
        default=10,
        ge=0,
        description="Timeout in seconds for graceful server shutdown.",
        validate_default=False,
    )
    limit_concurrency: Optional[int] = Field(
        default=50,
        ge=1,
        description="Maximum number of concurrent connections or tasks.",
    )
    workers: Optional[int] = Field(
        None, ge=1, description="Number of worker processes."
    )
    reload: Optional[bool] = Field(
        default=False, description="Enable auto-reloading for development."
    )
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None

    @property
    def host_str(self) -> str:
        return self.host.compressed


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

    server: UvicornServerSettings = Field(
        default_factory=UvicornServerSettings,
        description="Uvicorn web server configuration.",
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
