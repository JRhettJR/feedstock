"""Application configuration.

This module loads the application settings from a configuration file in
a pydantic BaseSettings object.

Attributes:
    settings: The application settings.
"""
from __future__ import annotations

from functools import lru_cache
from os import PathLike
from pathlib import Path
from typing import Any, Dict, Tuple, Type

import yaml
from pydantic.fields import FieldInfo
from pydantic.networks import HttpUrl
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

config_base_path: Path = Path(__file__).parents[2].resolve() / "application.yaml"


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """
    A simple settings source class that loads variables from a JSON file
    at the project's root.

    Here we happen to choose to use the `env_file_encoding` from Config
    when reading `config.json`
    """

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        encoding = self.config.get("env_file_encoding")
        yaml_config_path: str = config_base_path.read_text(encoding)
        file_content_json = yaml.safe_load(yaml_config_path)
        field_value = file_content_json.get(field_name)
        return field_value, field_name, False

    def prepare_field_value(
        self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool
    ) -> Any:
        return value

    def __call__(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}

        for field_name, field in self.settings_cls.model_fields.items():
            field_value, field_key, value_is_complex = self.get_field_value(
                field, field_name
            )
            field_value = self.prepare_field_value(
                field_name, field, field_value, value_is_complex
            )
            if field_value is not None:
                d[field_key] = field_value

        return d


class DataPrep(BaseSettings):
    source_path: str | Path | PathLike
    dest_path: str | Path | PathLike


class SoilTemperatureAPI(BaseSettings):
    url: HttpUrl


class GCSInfo(BaseSettings):
    project_id: str | Path | PathLike
    bucket_name: str | Path | PathLike


class FeedstockBucketFolders(BaseSettings):
    bulk_templates: str | Path | PathLike
    cleaned_data: str | Path | PathLike
    field_decisions: str | Path | PathLike
    mapping_data: str | Path | PathLike
    merged_data: str | Path | PathLike
    raw_data: str | Path | PathLike
    reporting: str | Path | PathLike
    support_data: str | Path | PathLike


class Settings(BaseSettings):
    """Collection of all settings definitions."""

    model_config = SettingsConfigDict(env_file_encoding="utf-8")

    data_prep: DataPrep
    soil_temperature_api: SoilTemperatureAPI
    gcs_dev: GCSInfo
    bucket_folders: FeedstockBucketFolders

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            YamlConfigSettingsSource(settings_cls),
            env_settings,
            file_secret_settings,
        )


@lru_cache
def get_settings() -> Settings:
    """Instantiates a configured settings object.

    Returns:
        A settings object with a loaded configuration.
    """
    return Settings()


settings: Settings = get_settings()
