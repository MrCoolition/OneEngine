from __future__ import annotations

import csv
import io
import json
import zipfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from .models import DistillationProfile, Document
from .normalization import clean_text


class SourceAdapter(Protocol):
    extensions: frozenset[str]

    def read(self, name: str, data: bytes) -> Sequence[Mapping[str, Any]]:
        ...


class AdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, SourceAdapter] = {}

    def register(self, adapter: SourceAdapter) -> None:
        for extension in adapter.extensions:
            self._adapters[extension.lower().lstrip(".")] = adapter

    def read(self, name: str, data: bytes) -> Sequence[Mapping[str, Any]]:
        extension = Path(name).suffix.lower().lstrip(".")
        adapter = self._adapters.get(extension)
        if adapter is None:
            supported = ", ".join(sorted(self._adapters))
            raise ValueError(
                f"No adapter registered for {name!r} ({extension or 'no extension'}). "
                f"Supported extensions: {supported}"
            )
        return adapter.read(name, data)

    @property
    def extensions(self) -> tuple[str, ...]:
        return tuple(sorted(self._adapters))


class DelimitedAdapter:
    extensions = frozenset({"csv", "tsv", "txt"})

    @staticmethod
    def _decode(data: bytes) -> str:
        for encoding in ("utf-8-sig", "utf-8", "cp1252"):
            try:
                return data.decode(encoding)
            except UnicodeDecodeError:
                continue
        raise ValueError("Delimited source encoding is not supported")

    def read(self, name: str, data: bytes) -> Sequence[Mapping[str, Any]]:
        text = self._decode(data)
        delimiter = "\t" if Path(name).suffix.lower() == ".tsv" else None
        if delimiter is None:
            try:
                delimiter = csv.Sniffer().sniff(text[:8192], delimiters=",\t;|").delimiter
            except csv.Error:
                delimiter = ","
        return tuple(dict(row) for row in csv.DictReader(io.StringIO(text), delimiter=delimiter))


class JsonAdapter:
    extensions = frozenset({"json", "jsonl", "ndjson"})

    def read(self, name: str, data: bytes) -> Sequence[Mapping[str, Any]]:
        text = data.decode("utf-8-sig")
        if Path(name).suffix.lower() in {".jsonl", ".ndjson"}:
            values = [json.loads(line) for line in text.splitlines() if line.strip()]
        else:
            payload = json.loads(text)
            if isinstance(payload, list):
                values = payload
            elif isinstance(payload, Mapping):
                candidate = payload.get("rows") or payload.get("records") or payload.get("data")
                values = candidate if isinstance(candidate, list) else [payload]
            else:
                raise ValueError(f"JSON source {name!r} must contain records")
        return tuple(dict(item) for item in values if isinstance(item, Mapping))


class PandasTabularAdapter:
    extensions = frozenset({"xlsx", "xlsm", "xls", "ods", "parquet", "feather"})

    def read(self, name: str, data: bytes) -> Sequence[Mapping[str, Any]]:
        try:
            import pandas as pd
        except Exception as exc:  # pragma: no cover - environment failure
            raise RuntimeError("The pandas adapter requires pandas") from exc

        extension = Path(name).suffix.lower()
        buffer = io.BytesIO(data)
        if extension in {".xlsx", ".xlsm", ".xls", ".ods"}:
            frame = pd.read_excel(buffer, sheet_name=0, dtype=object)
        elif extension == ".parquet":
            frame = pd.read_parquet(buffer)
        elif extension == ".feather":
            frame = pd.read_feather(buffer)
        else:  # pragma: no cover - registry guards this
            raise ValueError(f"Unsupported tabular extension: {extension}")
        frame = frame.where(frame.notna(), None)
        return tuple(frame.to_dict(orient="records"))


def default_registry() -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(DelimitedAdapter())
    registry.register(JsonAdapter())
    registry.register(PandasTabularAdapter())
    return registry


def canonicalize_row(
    row: Mapping[str, Any],
    profile: DistillationProfile,
) -> dict[str, Any]:
    aliases = {
        clean_text(key).lower(): value for key, value in profile.column_aliases.items()
    }
    output: dict[str, Any] = {}
    for raw_key, value in row.items():
        key = clean_text(raw_key)
        canonical = aliases.get(key.lower(), key)
        if canonical not in output or not clean_text(output[canonical]):
            output[canonical] = value
    return output


def documents_from_archive(
    path: str | Path,
    profile: DistillationProfile,
    *,
    registry: AdapterRegistry | None = None,
    group_resolver: Callable[[str], str] | None = None,
) -> tuple[Document, ...]:
    archive_path = Path(path)
    registry = registry or default_registry()
    resolver = group_resolver or (lambda name: Path(name).stem)
    documents: list[Document] = []
    with zipfile.ZipFile(archive_path) as archive:
        for member in sorted(archive.namelist()):
            if member.endswith("/"):
                continue
            extension = Path(member).suffix.lower().lstrip(".")
            if extension not in registry.extensions:
                continue
            rows = tuple(
                canonicalize_row(row, profile)
                for row in registry.read(member, archive.read(member))
            )
            base_name = Path(member).name
            documents.append(
                Document(
                    name=base_name,
                    source_type=extension,
                    rows=rows,
                    source_group=resolver(base_name),
                    metadata={
                        "archive": str(archive_path),
                        "member": member,
                        "row_count": len(rows),
                    },
                )
            )
    return tuple(documents)


def documents_from_path(
    path: str | Path,
    profile: DistillationProfile,
    *,
    registry: AdapterRegistry | None = None,
    group_resolver: Callable[[str], str] | None = None,
) -> tuple[Document, ...]:
    """Read a ZIP archive, directory tree, or individual supported source."""
    source_path = Path(path)
    registry = registry or default_registry()
    resolver = group_resolver or (lambda name: Path(name).stem)
    if source_path.is_file() and zipfile.is_zipfile(source_path):
        return documents_from_archive(
            source_path,
            profile,
            registry=registry,
            group_resolver=resolver,
        )

    if source_path.is_dir():
        candidates = sorted(
            candidate
            for candidate in source_path.rglob("*")
            if candidate.is_file()
            and candidate.suffix.lower().lstrip(".") in registry.extensions
        )
    elif source_path.is_file():
        candidates = [source_path]
    else:
        raise FileNotFoundError(f"Source path not found: {source_path}")

    documents: list[Document] = []
    for candidate in candidates:
        extension = candidate.suffix.lower().lstrip(".")
        if extension not in registry.extensions:
            continue
        rows = tuple(
            canonicalize_row(row, profile)
            for row in registry.read(candidate.name, candidate.read_bytes())
        )
        relative_name = (
            candidate.relative_to(source_path).as_posix()
            if source_path.is_dir()
            else candidate.name
        )
        documents.append(
            Document(
                name=candidate.name,
                source_type=extension,
                rows=rows,
                source_group=resolver(candidate.name),
                metadata={
                    "source": str(source_path),
                    "member": relative_name,
                    "row_count": len(rows),
                },
            )
        )
    return tuple(documents)
