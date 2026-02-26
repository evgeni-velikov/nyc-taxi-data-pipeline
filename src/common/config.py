from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List


@dataclass
class Config:
    # =========================
    # Storage
    # =========================
    storage: str = "s3a://sm-nyc-taxi-tripdata"
    raw_file_format: str = "parquet"

    # =========================
    # Catalog / Schemas
    # =========================
    catalog_name: str = "spark_catalog"
    schemas: Dict[str, str] = field(
        default_factory=lambda: {
            "bronze": "bronze",
            "silver": "silver",
            "gold": "gold"
        }
    )

    # =========================
    # Tables
    # =========================
    suffix_table: str = "trip_data"
    bronze_tables: List[str] = field(
        default_factory=lambda: ["fhv", "yellow", "green"]
    )

    # =========================
    # Bootstrap configuration
    # =========================
    starting_year: int = 2020
    starting_month: int = 1

    # =========================
    # Computed properties
    # =========================
    @property
    def raw_folder(self) -> str:
        return f"{self.storage}/raw"

    @property
    def incremental_folder(self) -> str:
        return f"{self.raw_folder}/incremental"

    @property
    def datasets_folder(self) -> str:
        return f"{self.storage}/datasets"

    @property
    def bronze_folder(self) -> str:
        return f"{self.datasets_folder}/bronze"

    @property
    def silver_folder(self) -> str:
        return f"{self.datasets_folder}/silver"

    @property
    def gold_folder(self) -> str:
        return f"{self.datasets_folder}/gold"

    @property
    def bronze_schema(self) -> str:
        return self.schemas["bronze"]

    @property
    def silver_schema(self) -> str:
        return self.schemas["silver"]

    @property
    def gold_schema(self) -> str:
        return self.schemas["gold"]

    @property
    def partition_date(self) -> date:
        return date(self.starting_year, self.starting_month, 1)

    @property
    def bootstrap_entry_point(self) -> str:
        return f"{self.starting_month:02d}_{self.starting_year}"
