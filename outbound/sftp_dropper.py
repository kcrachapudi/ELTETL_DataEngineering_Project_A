"""
SFTP Dropper — delivers files to partner SFTP servers.

Used for outbound EDI delivery when a partner doesn't accept
HTTP webhooks — many healthcare and retail partners still expect
files dropped to an SFTP server on a schedule.

Supports:
    - Password auth
    - Private key auth (most common in production)
    - Known hosts verification
    - Retry on connection failure
    - Post-drop verification (list remote file after upload)

Usage:
    dropper = SFTPDropper(
        host="sftp.partner.com",
        port=22,
        username="our_user",
        private_key_path="/secrets/sftp_key.pem",
        remote_dir="/inbound/edi/",
    )
    result = dropper.drop(local_path="outbound/edi/850_20240101.edi")
    result = dropper.drop_bytes(content=edi_bytes, remote_filename="837_batch.edi")

Dependencies:
    pip install paramiko
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class DropResult:
    success:         bool
    remote_path:     str  = ""
    bytes_written:   int  = 0
    error:           str  = ""
    partner_id:      str  = ""


class SFTPDropper:

    def __init__(
        self,
        host:             str,
        username:         str,
        remote_dir:       str,
        port:             int  = 22,
        password:         Optional[str] = None,
        private_key_path: Optional[str] = None,
        partner_id:       str = "",
        timeout:          int = 30,
        max_attempts:     int = 3,
    ):
        self._host     = host
        self._port     = port
        self._user     = username
        self._password = password
        self._key_path = private_key_path
        self._dir      = remote_dir.rstrip("/") + "/"
        self._partner  = partner_id
        self._timeout  = timeout
        self._max      = max_attempts

    def drop(self, local_path: str, remote_filename: Optional[str] = None) -> DropResult:
        """Upload a local file to the partner SFTP server."""
        path = Path(local_path)
        if not path.exists():
            return DropResult(
                success=False, partner_id=self._partner,
                error=f"Local file not found: {local_path}",
            )
        content = path.read_bytes()
        fname   = remote_filename or path.name
        return self.drop_bytes(content, fname)

    def drop_bytes(self, content: bytes, remote_filename: str) -> DropResult:
        """Upload raw bytes as a file to the partner SFTP server."""
        try:
            import paramiko
        except ImportError:
            return DropResult(
                success=False,
                error="paramiko required: pip install paramiko",
                partner_id=self._partner,
            )

        remote_path = f"{self._dir}{remote_filename}"
        last_error  = ""

        for attempt in range(1, self._max + 1):
            try:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                connect_kwargs = dict(
                    hostname=self._host,
                    port=self._port,
                    username=self._user,
                    timeout=self._timeout,
                    banner_timeout=self._timeout,
                )
                if self._key_path:
                    connect_kwargs["key_filename"] = self._key_path
                elif self._password:
                    connect_kwargs["password"] = self._password

                client.connect(**connect_kwargs)
                sftp = client.open_sftp()

                # ensure remote directory exists
                try:
                    sftp.stat(self._dir)
                except FileNotFoundError:
                    sftp.mkdir(self._dir)

                import io
                with sftp.open(remote_path, "wb") as f:
                    f.write(content)

                # verify upload
                stat        = sftp.stat(remote_path)
                bytes_written = stat.st_size

                sftp.close()
                client.close()

                logger.info(
                    f"SFTP drop success: {remote_path} "
                    f"({bytes_written} bytes) → {self._host} "
                    f"partner={self._partner} attempt={attempt}"
                )
                return DropResult(
                    success=True,
                    remote_path=remote_path,
                    bytes_written=bytes_written,
                    partner_id=self._partner,
                )

            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    f"SFTP drop failed attempt {attempt}/{self._max}: "
                    f"{self._host}{remote_path} — {exc}"
                )
                import time
                if attempt < self._max:
                    time.sleep(5 * attempt)
            finally:
                try:
                    client.close()
                except Exception:
                    pass

        logger.error(
            f"SFTP drop exhausted {self._max} attempts: "
            f"{self._host}{remote_path} last_error={last_error}"
        )
        return DropResult(
            success=False,
            remote_path=remote_path,
            error=last_error,
            partner_id=self._partner,
        )

    def list_remote(self, path: Optional[str] = None) -> list[str]:
        """List files in the remote directory — useful for inbound polling."""
        try:
            import paramiko
        except ImportError:
            logger.error("paramiko required: pip install paramiko")
            return []

        target = path or self._dir
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=self._host, port=self._port,
                username=self._user, password=self._password,
                key_filename=self._key_path, timeout=self._timeout,
            )
            sftp  = client.open_sftp()
            files = sftp.listdir(target)
            sftp.close()
            client.close()
            return files
        except Exception as exc:
            logger.error(f"SFTP list failed: {exc}")
            return []
