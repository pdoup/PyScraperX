import logging
from collections import namedtuple
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, List, Optional, Set

from pydantic import (
    AnyUrl,
    BaseModel,
    BeforeValidator,
    Field,
    HttpUrl,
    IPvAnyAddress,
    ValidationError,
    field_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("WebScraper")


class LogLevel(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

    def to_logging_level(self) -> int:
        """Convert to the corresponding logging module level."""
        return {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.WARNING: logging.WARNING,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.CRITICAL: logging.CRITICAL,
        }[self]

    @classmethod
    def _missing_(cls, value: Any):
        """Allows coercion from string (case-insensitive) and validates."""
        if isinstance(value, str):
            normalized = value.lower()
            for level in cls:
                if level.value == normalized:
                    return level
        raise ValueError(
            f"Invalid log level: {value!r}. Must be one of: {', '.join([l.value for l in cls])}"
        )


def validate_log_level(v: Any) -> str:
    """Validates if the provided value is a valid log level."""
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
    urls: Set[HttpUrl]

    @field_validator("urls", mode="before")
    @classmethod
    def log_duplicates(cls, v: List[str]) -> Set[HttpUrl]:
        """
        Logs the number of duplicate URLs found in the input list before
        Pydantic converts them to a unique set.
        """
        if not isinstance(v, list):
            return v

        unique_urls = set(v)
        duplicate_count = len(v) - len(unique_urls)
        if duplicate_count > 0:
            logger.warning(
                f"Found {duplicate_count} duplicate URL(s) in the input file. They will be ignored."
            )
        return unique_urls


ResolveTime = namedtuple("ResolveTime", ("interval", "unit"), defaults=(30, "seconds"))


def load_urls(filepath: Path, ignore_starts_with: Optional[str] = "#") -> Config:
    """
    Loads URLs from a text file, ignoring empty lines and lines that start with a specified prefix.

    Args:
        filepath (Path): The path to the text file containing the URLs.
        ignore_starts_with (Optional[str]): A string prefix to ignore lines that start with.
                                            Defaults to "#". If None, no lines are ignored based on prefix.

    Returns:
        Config: A Config object containing the loaded URLs.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        PermissionError: If there are insufficient permissions to read the file.
        OSError: For other operating system-related errors during file access.
        ValidationError: If the loaded URLs do not conform to the Config model's validation rules.
        Exception: For any other unexpected errors during file processing.
    """
    try:
        with filepath.open(mode="r", encoding="utf-8") as f:
            lines = [
                line_s
                for line in f
                if (line_s := line.strip())
                and (
                    ignore_starts_with is None
                    or not line_s.startswith(ignore_starts_with)
                )
            ]

        if not lines:
            logger.warning(
                f"No valid URLs found in '{filepath.resolve(strict=False)}'. File might be empty or contain only ignored lines."
            )

        logger.debug(f"Loaded {len(lines)} URLs from '{filepath.name}'.")

        return Config(urls=lines)

    except FileNotFoundError:
        logger.error(
            f"Error: URL file not found at '{filepath.resolve()}'. Please check the path."
        )
        raise
    except PermissionError:
        logger.error(
            f"Error: Insufficient permissions to read URL file at '{filepath.resolve()}'."
        )
        raise
    except OSError as e:
        logger.error(
            f"Error: An operating system error occurred while reading '{filepath.resolve()}': {e}"
        )
        raise
    except ValidationError as e:
        logger.error(
            f"Error: Invalid URL format in '{filepath.resolve()}'. Validation details: {e}"
        )
        raise
    except Exception as e:
        logger.exception(
            f"Unexpected error while processing URL file '{filepath.resolve()}': {e}"
        )
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

    debug: Optional[bool] = Field(
        default=False,
        description="Use debug mode for FastAPI",
    )

    master_scheduler_id: Optional[str] = Field(
        default="main_scheduler_job",
        description="ID of the master job",
        min_length=1,
        max_length=100,
        strict=True,
        frozen=True,
    )

    restart_all_timeout_seconds: Optional[float] = Field(
        default=60.0,
        description="Max timeout in seconds for restarting permanently failed jobs",
        gt=0,
        allow_inf_nan=False,
        validate_default=False,
    )

    http_client_timeout_seconds: Optional[float] = Field(
        default=10.0,
        description="Max response timeout in seconds for HTTP requests",
        gt=0,
        allow_inf_nan=False,
        validate_default=False,
    )

    log_level: LogLevel = Field(
        default=LogLevel.INFO,
        description="Logging level for main logger (debug, info, warning, error, critical)",
        frozen=True,
        validate_default=False,
    )

    concurrent_scrapers: Optional[int] = Field(
        default=10,
        description="Upper limit to concurrent scraping tasks",
        frozen=True,
        validate_default=False,
        gt=0,
        allow_inf_nan=False,
    )

    server: UvicornServerSettings = Field(
        default_factory=UvicornServerSettings,
        description="Uvicorn web server configuration.",
    )

    http_proxy: Optional[AnyUrl] = Field(
        default=None, env="HTTP_PROXY", frozen=True, description="URL of the HTTP Proxy"
    )
    https_proxy: Optional[AnyUrl] = Field(
        default=None,
        env="HTTPS_PROXY",
        frozen=True,
        description="URL of the HTTPS Proxy",
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
