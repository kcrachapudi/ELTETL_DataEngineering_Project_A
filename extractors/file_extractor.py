"""
File Extractor — reads files from local disk, GCS, S3, or SFTP.

Returns raw bytes — pass to the appropriate parser based on file extension.

Usage:
    # Local file
    ext = FileExtractor("sample_data/sample_850.edi")
    raw = ext.fetch()
    df  = EDIParser().parse(raw)

    # GCS bucket (Project 4)
    ext = FileExtractor("gs://your-bucket/raw/orders/2024-01-15.csv")
    raw = ext.fetch()
    df  = CSVParser().parse(raw)

    # SFTP (requires paramiko)
    ext = FileExtractor(
        "sftp://user@sftp.partner.com/inbound/834_20240101.edi",
        sftp_key_path="/secrets/sftp_key.pem",
    )
    raw = ext.fetch()
    df  = EDI834Parser().parse(raw)
"""

import logging
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from .base_extractor import BaseExtractor, ExtractorError

logger = logging.getLogger(__name__)


class FileExtractor(BaseExtractor):
    """
    Reads a file from local disk, GCS, S3, or SFTP.
    Returns raw bytes — caller passes to appropriate parser.
    """

    def __init__(
        self,
        path:             str,
        sftp_username:    Optional[str] = None,
        sftp_password:    Optional[str] = None,
        sftp_key_path:    Optional[str] = None,
        encoding:         str           = "utf-8",
    ):
        self._path         = path
        self._sftp_user    = sftp_username
        self._sftp_pass    = sftp_password
        self._sftp_key     = sftp_key_path
        self._encoding     = encoding

    @property
    def source_name(self) -> str:
        return self._path

    def fetch(self) -> bytes:
        """
        Read file and return raw bytes.
        Scheme is inferred from the path prefix:
            gs://    → Google Cloud Storage
            s3://    → AWS S3
            sftp://  → SFTP server
            (none)   → local filesystem
        """
        path = self._path.strip()

        if path.startswith("gs://"):
            return self._fetch_gcs(path)
        elif path.startswith("s3://"):
            return self._fetch_s3(path)
        elif path.startswith("sftp://"):
            return self._fetch_sftp(path)
        else:
            return self._fetch_local(path)

    def _fetch_local(self, path: str) -> bytes:
        p = Path(path)
        if not p.exists():
            raise ExtractorError(f"File not found: {path}")
        data = p.read_bytes()
        logger.info(f"Local file read: {path} ({len(data)} bytes)")
        return data

    def _fetch_gcs(self, uri: str) -> bytes:
        """Read from Google Cloud Storage. Requires google-cloud-storage."""
        try:
            from google.cloud import storage as gcs
        except ImportError:
            raise ExtractorError(
                "google-cloud-storage required for GCS: "
                "pip install google-cloud-storage"
            )
        parsed = urlparse(uri)
        bucket_name = parsed.netloc
        blob_path   = parsed.path.lstrip("/")
        client = gcs.Client()
        bucket = client.bucket(bucket_name)
        blob   = bucket.blob(blob_path)
        data   = blob.download_as_bytes()
        logger.info(f"GCS read: {uri} ({len(data)} bytes)")
        return data

    def _fetch_s3(self, uri: str) -> bytes:
        """Read from AWS S3. Requires boto3."""
        try:
            import boto3
        except ImportError:
            raise ExtractorError("boto3 required for S3: pip install boto3")
        parsed = urlparse(uri)
        bucket = parsed.netloc
        key    = parsed.path.lstrip("/")
        s3     = boto3.client("s3")
        resp   = s3.get_object(Bucket=bucket, Key=key)
        data   = resp["Body"].read()
        logger.info(f"S3 read: {uri} ({len(data)} bytes)")
        return data

    def _fetch_sftp(self, uri: str) -> bytes:
        """Read from SFTP server. Requires paramiko."""
        try:
            import paramiko, io
        except ImportError:
            raise ExtractorError("paramiko required for SFTP: pip install paramiko")
        parsed = urlparse(uri)
        host   = parsed.hostname
        port   = parsed.port or 22
        user   = self._sftp_user or parsed.username or ""
        path   = parsed.path

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            connect_kwargs = dict(hostname=host, port=port, username=user, timeout=30)
            if self._sftp_key:
                connect_kwargs["key_filename"] = self._sftp_key
            elif self._sftp_pass:
                connect_kwargs["password"] = self._sftp_pass
            client.connect(**connect_kwargs)
            sftp = client.open_sftp()
            buf  = io.BytesIO()
            sftp.getfo(path, buf)
            data = buf.getvalue()
            sftp.close()
            logger.info(f"SFTP read: {uri} ({len(data)} bytes)")
            return data
        finally:
            client.close()
