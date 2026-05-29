import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd


@dataclass
class PackageResult:
    name: str
    success: bool
    rows_loaded: int
    warnings: list = field(default_factory=list)


class ETLPackage(ABC):
    """
    Base para cada fuente del ETL.
    Escribe a profiling.run_log en start/finish y persiste column_stats.
    """

    def __init__(self, conn, minio_config: dict, partition_date: str):
        self.conn = conn
        self.minio_config = minio_config
        self.partition_date = partition_date
        self.log = logging.getLogger(self.__class__.__name__)
        self._run_id: int | None = None

    def run(self) -> PackageResult:
        self.log.info(f"=== {self.__class__.__name__} ===")
        self._run_id = self._start_run()

        try:
            data = self.extract()
            rows = _row_count(data)

            if rows == 0:
                self.log.warning("Sin datos — package omitido")
                self._finish_run(rows_extracted=0, rows_loaded=0, status="skipped")
                return PackageResult(name=self.__class__.__name__, success=True, rows_loaded=0)

            warnings = self.profile(data)
            for w in warnings:
                self.log.warning(f"  [profile] {w}")

            self.stage(data)
            self.load()

            self._finish_run(rows_extracted=rows, rows_loaded=rows,
                             warnings_count=len(warnings), status="ok")
            self.log.info(f"=== {self.__class__.__name__} OK — {rows:,} filas ===")
            return PackageResult(
                name=self.__class__.__name__,
                success=True,
                rows_loaded=rows,
                warnings=warnings,
            )

        except Exception as e:
            self._finish_run(rows_extracted=0, rows_loaded=0, status="failed", error_msg=str(e))
            raise

    # ------------------------------------------------------------------ run_log

    def _start_run(self) -> int | None:
        try:
            from datetime import date as dt
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO profiling.run_log
                        (partition_date, package_name, started_at, status)
                    VALUES (%s, %s, now(), 'running')
                    RETURNING run_id
                """, (self.partition_date, self.__class__.__name__))
                run_id = cur.fetchone()[0]
            self.conn.commit()
            return run_id
        except Exception as e:
            self.log.debug(f"[run_log] No se pudo escribir start: {e}")
            return None

    def _finish_run(self, rows_extracted: int = 0, rows_loaded: int = 0,
                    warnings_count: int = 0, status: str = "ok",
                    error_msg: str | None = None) -> None:
        if self._run_id is None:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE profiling.run_log
                    SET finished_at    = now(),
                        rows_extracted = %s,
                        rows_loaded    = %s,
                        warnings_count = %s,
                        status         = %s,
                        error_msg      = %s
                    WHERE run_id = %s
                """, (rows_extracted, rows_loaded, warnings_count,
                      status, error_msg, self._run_id))
            self.conn.commit()
        except Exception as e:
            self.log.debug(f"[run_log] No se pudo escribir finish: {e}")

    # ------------------------------------------------------------------ abstract

    @abstractmethod
    def extract(self) -> pd.DataFrame | dict: ...

    @abstractmethod
    def profile(self, data: pd.DataFrame | dict) -> list[str]: ...

    @abstractmethod
    def stage(self, data: pd.DataFrame | dict) -> None: ...

    @abstractmethod
    def load(self) -> None: ...


def _row_count(data) -> int:
    if isinstance(data, pd.DataFrame):
        return len(data)
    if isinstance(data, dict):
        return max((len(v) for v in data.values() if isinstance(v, pd.DataFrame)), default=0)
    return 0
