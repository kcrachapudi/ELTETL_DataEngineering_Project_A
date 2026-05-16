"""
Settings — all configuration from environment variables.

Never hardcode credentials. Everything here reads from environment
variables with sensible defaults for local development.

Usage:
    from config.settings import settings
    conn_str = settings.database_url
    secret   = settings.api_secret_key

Local dev: copy .env.example to .env, fill in values, run:
    export $(cat .env | xargs) && python run.py

Docker: pass env vars via docker-compose.yml environment section.
GCP:    use Secret Manager — values injected at runtime.
"""

import os
from dataclasses import dataclass, field


@dataclass
class Settings:

    # ── Database ─────────────────────────────────────────────────────────
    db_host:     str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    db_port:     int = field(default_factory=lambda: int(os.getenv("DB_PORT", "5432")))
    db_name:     str = field(default_factory=lambda: os.getenv("DB_NAME", "pipeline_db"))
    db_user:     str = field(default_factory=lambda: os.getenv("DB_USER", "pipeline_user"))
    db_password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", "changeme"))

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def psycopg2_dsn(self) -> str:
        return (
            f"host={self.db_host} port={self.db_port} "
            f"dbname={self.db_name} user={self.db_user} "
            f"password={self.db_password}"
        )

    # ── API server ────────────────────────────────────────────────────────
    api_host:       str = field(default_factory=lambda: os.getenv("API_HOST", "0.0.0.0"))
    api_port:       int = field(default_factory=lambda: int(os.getenv("API_PORT", "8000")))
    api_secret_key: str = field(default_factory=lambda: os.getenv("API_SECRET_KEY", "dev-secret-change-in-prod"))
    api_debug:      bool = field(default_factory=lambda: os.getenv("API_DEBUG", "false").lower() == "true")

    # ── GCP (Project 4) ───────────────────────────────────────────────────
    gcp_project:    str = field(default_factory=lambda: os.getenv("GCP_PROJECT", ""))
    gcp_dataset:    str = field(default_factory=lambda: os.getenv("GCP_DATASET", "raw"))
    gcs_bucket:     str = field(default_factory=lambda: os.getenv("GCS_BUCKET", ""))
    gcp_location:   str = field(default_factory=lambda: os.getenv("GCP_LOCATION", "US"))

    # ── Logging ───────────────────────────────────────────────────────────
    log_level:  str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_format: str = "%(asctime)s %(levelname)s %(name)s — %(message)s"

    # ── Pipeline behaviour ────────────────────────────────────────────────
    batch_chunk_size:  int = field(default_factory=lambda: int(os.getenv("BATCH_CHUNK_SIZE", "1000")))
    retry_max_attempts: int = field(default_factory=lambda: int(os.getenv("RETRY_MAX_ATTEMPTS", "6")))
    idempotency_ttl_days: int = field(default_factory=lambda: int(os.getenv("IDEMPOTENCY_TTL_DAYS", "7")))

    # ── Weather API (Project 1 data source) ───────────────────────────────
    open_meteo_base_url: str = "https://api.open-meteo.com/v1"
    open_meteo_latitude:  float = field(default_factory=lambda: float(os.getenv("LATITUDE", "32.7767")))   # Dallas default
    open_meteo_longitude: float = field(default_factory=lambda: float(os.getenv("LONGITUDE", "-96.7970")))

    def validate(self):
        """Call at startup to catch missing required config early."""
        required = []
        if not self.db_password or self.db_password == "changeme":
            required.append("DB_PASSWORD")
        if self.api_secret_key == "dev-secret-change-in-prod":
            import logging
            logging.getLogger(__name__).warning(
                "API_SECRET_KEY is using the default dev value — set it in production."
            )
        if required:
            raise EnvironmentError(f"Missing required env vars: {required}")

    def get_db_connection(self):
        """Return a live psycopg2 connection using current settings."""
        try:
            import psycopg2
            return psycopg2.connect(self.psycopg2_dsn)
        except ImportError:
            raise ImportError("psycopg2 required: pip install psycopg2-binary")


# Singleton — import this everywhere
settings = Settings()
