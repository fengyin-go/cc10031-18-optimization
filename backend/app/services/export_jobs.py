"""分段导出作业 — 断点续写、分段校验和与原子文件发布。

背景
----
原来的导出接口把全部行拼成一个内存字符串后一次性返回：批次一大就占满内存，
下载中断后只能从头重来，也无法判断哪些字段映射、哪些记录已经成功落盘。

本模块把一次导出拆成可独立提交的分段：

1. ``create_job`` 读取采集记录，按窗口（批次/记录 ID 区间/offset+limit）
   套用输出模板的字段映射，流式写出 ``source.jsonl`` 快照和 ``plan.json``，
   同时逐段计算校验和（不保留整份字符串）。
2. ``run_job`` 只写缺失或校验和不匹配的分段；已有且校验通过的分段直接跳过，
   因此失败恢复不会重新下载/重写已确认的数据分段。
3. 每个分段先写临时文件、fsync，再 ``os.replace`` 原子落位；确认状态最后写入
   作业状态文件。所有分段确认后才拼装最终文件，并**最后**原子写入
   ``manifest.json`` 作为完成标记 —— 没有完成标记的导出不会出现在可下载列表，
   也不会触发交付回执。
4. 同一指纹（模板 + 窗口 + 分段大小 + 记录快照）的导出只接受首个成功结果：
   后来的新版本在提交时发现已有完成版本，会落到 ``superseded/conflict`` 终态，
   不发布文件、不产生回执。

磁盘布局::

    <root>/jobs/<job_id>/
        plan.json          # 不可变：模板规格、分段边界、每段校验和
        source.jsonl       # 不可变：窗口内已格式化行的快照（分段按字节区间切片）
        state.json         # 可变：每段保存状态、写入次数、交付状态
        segments/seg-00001.csv
        final/<job_id>.csv
        manifest.json      # 完成标记（原子提交，最后写）
    <root>/receipts.json   # 交付回执登记（保证每个任务最多一条）

``manifest.json`` 是唯一对外暴露“已完成”的依据：只有它存在且校验和自洽，
``list_jobs(downloadable_only=True)`` 才会返回该任务，回调钩子才会生成回执。
"""
from __future__ import annotations

import codecs
import hashlib
import json
import os
import re
import shutil
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.store import store

SCHEMA_VERSION = 1
EXPORT_TOPIC = "rpa.export_completed"
CONFLICT_TOPIC = "rpa.export_conflict"
RECEIPT_TOPIC = "rpa.export_delivery_receipt"

DEFAULT_SEGMENT_SIZE = 200
MAX_SEGMENT_SIZE = 5000
MAX_ROWS = 1_000_000

JOB_ID_RE = re.compile(r"^EXP-[0-9a-f]{10}-v\d+$")
_INTERNAL_FIELD_PREFIX = "_"

_NULL_SENTINEL = object()

_DELIMITER_ALIASES = {
    "\\t": "\t",
    "tab": "\t",
    "tsv": "\t",
    "comma": ",",
    "semicolon": ";",
    "pipe": "|",
    "空格": " ",
    "逗号": ",",
    "分号": ";",
    "制表符": "\t",
    "竖线": "|",
}

_TRUE_VALUES = {"1", "true", "yes", "on", "是", "开启", "展开", "启用"}

_MEDIA_TYPES = {
    "csv": "text/csv",
    "tsv": "text/tab-separated-values",
    "jsonl": "application/x-ndjson",
    "ndjson": "application/x-ndjson",
    "txt": "text/plain",
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> tuple[str, int]:
    """流式计算文件摘要，避免把大文件整体读进内存。"""
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _canonical_json(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """临时文件 + fsync + os.replace，避免读者看到半个文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with open(tmp, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_bytes(
        path,
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8"),
    )


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_bytes().decode("utf-8"))
    except (OSError, ValueError):
        return None


class ExportJobError(Exception):
    """导出作业的显式错误，路由层可据此返回 4xx。"""

    def __init__(self, message: str, *, code: str = "invalid_request") -> None:
        super().__init__(message)
        self.code = code


class ExportInterrupted(RuntimeError):
    """用于验证断点续写的可控中断（只在内部/测试注入）。"""


# ---------------------------------------------------------------------------
# 模板解析与行格式化
# ---------------------------------------------------------------------------

def _is_placeholder(value: Any) -> bool:
    """示例数据里的样板值（如“输出格式样例1”）不是有效配置。"""
    text = str(value or "").strip()
    return not text or text.endswith("样例1") or text.endswith("样例2") or text.endswith("样例3")


def _parse_field_mapping(raw: Any) -> dict[str, str]:
    """解析字段映射。

    支持 JSON 对象、``源:目标`` / ``源=目标`` / ``源->目标`` 分隔的映射；
    示例数据里的占位字符串会被忽略，此时按记录字段原样输出。
    """
    text = str(raw or "").strip()
    if not text or _is_placeholder(text):
        return {}
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        parsed = None
    if isinstance(parsed, dict):
        pairs = {
            str(key).strip(): str(value).strip()
            for key, value in parsed.items()
            if str(key).strip() and str(value).strip()
        }
        return pairs

    pairs = {}
    for chunk in re.split(r"[;；,，\n]+", text):
        chunk = chunk.strip()
        if not chunk:
            continue
        for separator in ("->", "→", ":", "：", "="):
            if separator in chunk:
                source, _, target = chunk.partition(separator)
                source, target = source.strip(), target.strip()
                if source and target:
                    pairs[source] = target
                break
    return pairs


def _resolve_output_type(raw: Any) -> str:
    text = str(raw or "").strip().lower()
    if _is_placeholder(text):
        return "csv"
    aliases = {
        "csv": "csv", "excel": "csv", "表格": "csv",
        "tsv": "tsv", "tab": "tsv",
        "jsonl": "jsonl", "ndjson": "jsonl", "json lines": "jsonl",
        "txt": "txt", "text": "txt", "纯文本": "txt",
    }
    return aliases.get(text, "csv")


def _resolve_delimiter(raw: Any, output_type: str) -> str:
    text = str(raw or "").strip()
    if not text or _is_placeholder(text):
        return "\t" if output_type == "tsv" else ","
    lowered = text.lower()
    if lowered in _DELIMITER_ALIASES:
        return _DELIMITER_ALIASES[lowered]
    if text in _DELIMITER_ALIASES:
        return _DELIMITER_ALIASES[text]
    if len(text) == 1:
        return text
    return "\t" if output_type == "tsv" else ","


def _resolve_encoding(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text or _is_placeholder(text):
        return "utf-8"
    try:
        return codecs.lookup(text).name
    except LookupError:
        return "utf-8"


def _resolve_null_placeholder(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text or _is_placeholder(text):
        return "NA"
    return text


def _resolve_nested(raw: Any) -> bool:
    return str(raw or "").strip().lower() in _TRUE_VALUES


class ExportFormatSpec:
    """一次导出的输出模板规格（与 store 中的行解耦，便于持久化）。"""

    def __init__(self, template: dict[str, Any]) -> None:
        self.template_id = int(template.get("id", 0))
        self.template_name = str(template.get("模板名称") or f"template-{self.template_id}")
        self.output_type = _resolve_output_type(template.get("输出类型"))
        self.delimiter = _resolve_delimiter(template.get("分隔符"), self.output_type)
        self.encoding = _resolve_encoding(template.get("编码方式"))
        self.null_placeholder = _resolve_null_placeholder(template.get("空值表示"))
        self.nested = _resolve_nested(template.get("嵌套展开"))
        self.mapping = _parse_field_mapping(template.get("字段映射"))

    @property
    def extension(self) -> str:
        return {"csv": "csv", "tsv": "tsv", "jsonl": "jsonl"}.get(self.output_type, "txt")

    @property
    def media_type(self) -> str:
        return _MEDIA_TYPES.get(self.output_type, "text/plain")

    def as_dict(self) -> dict[str, Any]:
        return {
            "template_id": self.template_id,
            "template_name": self.template_name,
            "output_type": self.output_type,
            "delimiter": self.delimiter,
            "encoding": self.encoding,
            "null_placeholder": self.null_placeholder,
            "nested": self.nested,
            "mapping": self.mapping,
            "extension": self.extension,
            "media_type": self.media_type,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ExportFormatSpec":
        spec = cls.__new__(cls)
        spec.template_id = int(payload.get("template_id", 0))
        spec.template_name = str(payload.get("template_name") or "template")
        spec.output_type = str(payload.get("output_type") or "csv")
        spec.delimiter = str(payload.get("delimiter") or ",")
        spec.encoding = str(payload.get("encoding") or "utf-8")
        spec.null_placeholder = str(payload.get("null_placeholder", "NA"))
        spec.nested = bool(payload.get("nested"))
        spec.mapping = {str(k): str(v) for k, v in (payload.get("mapping") or {}).items()}
        return spec


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    """把嵌套字典/列表展开成点号路径字段，用于列发现与取值。"""
    flat: dict[str, Any] = {}
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            flat.update(_flatten(item, child))
        if not value and prefix:
            flat[prefix] = None
    elif isinstance(value, list):
        if prefix:
            flat[prefix] = json.dumps(value, ensure_ascii=False)
        else:
            for index, item in enumerate(value):
                flat.update(_flatten(item, str(index)))
    else:
        if prefix:
            flat[prefix] = value
    return flat


def _source_view(fields: dict[str, Any], nested: bool) -> dict[str, Any]:
    return _flatten(fields) if nested else dict(fields)


def _lookup(fields: dict[str, Any], key: str, nested: bool) -> Any:
    if key in fields:
        return fields[key]
    if nested and "." in key:
        current: Any = fields
        for part in key.split("."):
            if not isinstance(current, dict) or part not in current:
                return _NULL_SENTINEL
            current = current[part]
        return current
    return _NULL_SENTINEL


def _cell_text(value: Any, null_placeholder: str, nested: bool) -> str:
    if value is _NULL_SENTINEL or value is None:
        return null_placeholder
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _guard_formula(text: str) -> str:
    if text and text[0] in ("=", "+", "-", "@"):
        return "'" + text
    return text


def _discover_columns(
    records: list[dict[str, Any]], spec: ExportFormatSpec,
) -> list[str]:
    if spec.mapping:
        return list(dict.fromkeys(spec.mapping.values()))
    columns: list[str] = []
    for fields in records:
        for key in _source_view(fields, spec.nested):
            if key.startswith(_INTERNAL_FIELD_PREFIX):
                continue
            if key not in columns:
                columns.append(key)
    return columns


def _format_cells(
    fields: dict[str, Any], spec: ExportFormatSpec, columns: list[str],
) -> list[str]:
    view = _source_view(fields, spec.nested)
    if spec.mapping:
        values = {
            target: _lookup(fields, source, spec.nested)
            for source, target in spec.mapping.items()
        }
        cells = [values.get(column, _NULL_SENTINEL) for column in columns]
    else:
        cells = [view.get(column, _NULL_SENTINEL) for column in columns]
    return [
        _guard_formula(_cell_text(cell, spec.null_placeholder, spec.nested))
        for cell in cells
    ]


def _csv_escape(value: str, delimiter: str) -> str:
    needs_quote = (
        delimiter in value
        or '"' in value
        or "\n" in value
        or "\r" in value
    )
    if not needs_quote:
        return value
    return '"' + value.replace('"', '""') + '"'


def _row_line(
    fields: dict[str, Any], spec: ExportFormatSpec, columns: list[str],
) -> str:
    cells = _format_cells(fields, spec, columns)
    if spec.output_type in ("jsonl", "ndjson"):
        return json.dumps(
            {column: cell for column, cell in zip(columns, cells)},
            ensure_ascii=False,
        )
    return spec.delimiter.join(_csv_escape(cell, spec.delimiter) for cell in cells)


def _header_line(spec: ExportFormatSpec, columns: list[str]) -> str:
    if spec.output_type in ("jsonl", "ndjson") or not columns:
        return ""
    return spec.delimiter.join(_csv_escape(column, spec.delimiter) for column in columns)


def _encode_line(line: str, spec: ExportFormatSpec) -> bytes:
    return line.encode(spec.encoding, errors="replace")


def _encode_header(line: str, spec: ExportFormatSpec) -> bytes:
    if not line:
        return b""
    encoded = _encode_line(line + "\n", spec)
    if spec.encoding in ("utf-8", "utf_8") and spec.output_type not in ("jsonl", "ndjson"):
        # 与既有 CSV 导出保持一致，带 BOM 方便表格软件识别编码。
        return b"\xef\xbb\xbf" + encoded
    return encoded


# ---------------------------------------------------------------------------
# 分段导出管理器
# ---------------------------------------------------------------------------

class ExportJobManager:
    """分段导出的创建、续写、原子发布与交付回执。"""

    def __init__(self, root: Path | str | None = None) -> None:
        default_root = Path(__file__).resolve().parents[2] / "var" / "exports"
        self._lock = threading.RLock()
        self._commit_lock = threading.RLock()
        self._locks: dict[str, threading.Lock] = {}
        self.configure(root if root is not None else os.environ.get("RPA_EXPORT_ROOT", default_root))

    # ---- 配置与基础路径 ----

    def configure(self, root: Path | str) -> None:
        """切换导出根目录（测试或多实例隔离时使用）。"""
        with self._lock:
            self.root = Path(root)
            self.root.mkdir(parents=True, exist_ok=True)
            self._locks = {}

    @property
    def jobs_root(self) -> Path:
        return self.root / "jobs"

    def _job_dir(self, job_id: str) -> Path:
        if not JOB_ID_RE.match(job_id or ""):
            raise ExportJobError("导出任务编号非法", code="job_not_found")
        return self.jobs_root / job_id

    def _job_lock(self, job_id: str) -> threading.Lock:
        with self._lock:
            return self._locks.setdefault(job_id, threading.Lock())

    # ---- 窗口与快照 ----

    @staticmethod
    def _window_entries(
        *,
        batch_ref: str | None,
        entry_start: int | None,
        entry_end: int | None,
        offset: int,
        limit: int | None,
    ) -> list[Any]:
        if batch_ref:
            entries = store.collect_records.list_by_batch(batch_ref)
        else:
            entries = store.collect_records.all_entries()
        if entry_start is not None:
            entries = [e for e in entries if e.entry_id >= int(entry_start)]
        if entry_end is not None:
            entries = [e for e in entries if e.entry_id <= int(entry_end)]
        entries = sorted(entries, key=lambda e: e.entry_id)
        if offset:
            entries = entries[offset:]
        if limit is not None:
            entries = entries[:limit]
        return entries

    # ---- 创建作业 ----

    def create_job(
        self,
        template_id: int,
        *,
        batch_ref: str | None = None,
        entry_start: int | None = None,
        entry_end: int | None = None,
        offset: int = 0,
        limit: int | None = None,
        segment_size: int = DEFAULT_SEGMENT_SIZE,
        new_version: bool = False,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        template = store.find("output_format", int(template_id))
        if template is None:
            raise ExportJobError(f"输出模板 {template_id} 不存在", code="template_not_found")

        segment_size = int(segment_size or 0)
        if segment_size < 1 or segment_size > MAX_SEGMENT_SIZE:
            raise ExportJobError(
                f"分段大小需在 1..{MAX_SEGMENT_SIZE} 之间", code="invalid_segment_size")
        if offset < 0:
            raise ExportJobError("offset 不能为负数", code="invalid_window")
        if limit is not None and int(limit) < 1:
            raise ExportJobError("limit 必须为正整数", code="invalid_window")

        entries = self._window_entries(
            batch_ref=batch_ref, entry_start=entry_start, entry_end=entry_end,
            offset=int(offset or 0), limit=int(limit) if limit is not None else None,
        )
        if len(entries) > MAX_ROWS:
            raise ExportJobError(
                f"单次导出最多 {MAX_ROWS} 行，请缩小窗口", code="window_too_large")

        spec = ExportFormatSpec(template)
        records = [dict(entry.fields) for entry in entries]
        columns = _discover_columns(records, spec)

        fingerprint = self._fingerprint(
            spec=spec, entries=entries, columns=columns,
            segment_size=segment_size, window={
                "batch_ref": batch_ref,
                "entry_start": entry_start,
                "entry_end": entry_end,
                "offset": int(offset or 0),
                "limit": int(limit) if limit is not None else None,
            },
        )

        with self._lock:
            # 同一指纹只接受首个成功结果：已有完成版本时直接复用，不再创建新版本。
            completed = self._completed_sibling({
                "fingerprint": fingerprint, "job_id": None})
            if completed is not None:
                job = self.get_job(completed)
                job["reused_existing"] = True
                return job
            if not new_version:
                existing = self._find_latest_by_fingerprint(fingerprint)
                if existing is not None:
                    job = self.get_job(existing)
                    job["reused_existing"] = True
                    return job
            version = self._next_version(fingerprint)

            job_id = f"EXP-{fingerprint[:10]}-v{version}"
            job_dir = self.jobs_root / job_id
            if not job_dir.exists():
                self._create_job_files(
                    job_id=job_id,
                    job_dir=job_dir,
                    spec=spec,
                    entries=entries,
                    columns=columns,
                    segment_size=segment_size,
                    fingerprint=fingerprint,
                    version=version,
                    idempotency_key=idempotency_key or fingerprint,
                    window={
                        "batch_ref": batch_ref,
                        "entry_start": entry_start,
                        "entry_end": entry_end,
                        "offset": int(offset or 0),
                        "limit": int(limit) if limit is not None else None,
                    },
                )
        return self.get_job(job_id)

    def _fingerprint(
        self,
        *,
        spec: ExportFormatSpec,
        entries: list[Any],
        columns: list[str],
        segment_size: int,
        window: dict[str, Any],
    ) -> str:
        source_hasher = hashlib.sha256()
        for entry in entries:
            source_hasher.update(str(entry.entry_id).encode())
            source_hasher.update(_canonical_json(entry.fields))
        payload = {
            "template": spec.as_dict(),
            "columns": columns,
            "segment_size": int(segment_size),
            "window": window,
            "source_digest": source_hasher.hexdigest(),
        }
        return _sha256_bytes(_canonical_json(payload))

    def _next_version(self, fingerprint: str) -> int:
        prefix = f"EXP-{fingerprint[:10]}-v"
        versions = []
        if self.jobs_root.exists():
            for path in self.jobs_root.iterdir():
                if not path.is_dir() or not path.name.startswith(prefix):
                    continue
                tail = path.name.rsplit("-v", 1)[1]
                if tail.isdigit():
                    versions.append(int(tail))
        return max(versions, default=0) + 1

    def _find_latest_by_fingerprint(self, fingerprint: str) -> str | None:
        prefix = f"EXP-{fingerprint[:10]}-v"
        candidates = sorted(
            (p.name for p in self.jobs_root.glob(f"{prefix}*") if p.is_dir()),
            key=lambda name: int(name.rsplit("-v", 1)[1]) if name.rsplit("-v", 1)[1].isdigit() else 0,
        )
        for name in reversed(candidates):
            if self._plan_fingerprint(name) == fingerprint:
                return name
        return None

    def _plan_fingerprint(self, job_id: str) -> str | None:
        plan = _read_json(self.jobs_root / job_id / "plan.json")
        return (plan or {}).get("fingerprint")

    def _create_job_files(
        self,
        *,
        job_id: str,
        job_dir: Path,
        spec: ExportFormatSpec,
        entries: list[Any],
        columns: list[str],
        segment_size: int,
        fingerprint: str,
        version: int,
        idempotency_key: str,
        window: dict[str, Any],
    ) -> None:
        """在 staging 目录一次性写快照与计划，然后整目录原子改名。"""
        staging = self.jobs_root / f".staging-{uuid.uuid4().hex}"
        staging.mkdir(parents=True, exist_ok=False)
        try:
            header = _header_line(spec, columns)
            header_bytes = _encode_header(header, spec)

            segment_hashers: list[hashlib._Hash] = []
            segment_bytes: list[int] = []
            segment_checksums: list[str] = []
            segment_meta: list[dict[str, Any]] = []
            row_offsets: list[int] = []
            source_path = staging / "source.jsonl"

            total_rows = len(entries)
            segment_count = (total_rows + segment_size - 1) // segment_size if total_rows else 0

            with open(source_path, "wb") as source:
                for index in range(total_rows):
                    if index % segment_size == 0:
                        hasher = hashlib.sha256()
                        written_bytes = 0
                        if index == 0 and header_bytes:
                            hasher.update(header_bytes)
                            written_bytes += len(header_bytes)
                        segment_hashers.append(hasher)
                        segment_bytes.append(written_bytes)
                        segment_meta.append({
                            "index": len(segment_hashers),
                            "row_start": index,
                            "row_end": min(index + segment_size, total_rows),
                            "row_count": min(segment_size, total_rows - index),
                        })
                    row_offsets.append(source.tell())
                    line = _row_line(entries[index].fields, spec, columns)
                    payload = (json.dumps(line, ensure_ascii=False) + "\n").encode("utf-8")
                    source.write(payload)
                    encoded = _encode_line(line + "\n", spec)
                    segment_hashers[-1].update(encoded)
                    segment_bytes[-1] += len(encoded)
                source.flush()
                os.fsync(source.fileno())
                source_size = source.tell()

            if total_rows == 0:
                source_size = 0

            for position, meta in enumerate(segment_meta):
                checksum = segment_hashers[position].hexdigest()
                segment_checksums.append(checksum)
                start_row = meta["row_start"]
                end_row = meta["row_end"]
                byte_start = row_offsets[start_row] if start_row < len(row_offsets) else source_size
                byte_end = row_offsets[end_row] if end_row < len(row_offsets) else source_size
                meta.update({
                    "checksum": checksum,
                    "bytes": segment_bytes[position],
                    "byte_start": byte_start,
                    "byte_end": byte_end,
                    "file": f"segments/seg-{meta['index']:05d}.{spec.extension}",
                    "status": "pending",
                })

            plan = {
                "schema_version": SCHEMA_VERSION,
                "job_id": job_id,
                "fingerprint": fingerprint,
                "version": version,
                "template": spec.as_dict(),
                "columns": columns,
                "header": header,
                "header_checksum": _sha256_bytes(header_bytes) if header_bytes else None,
                "window": window,
                "segment_size": segment_size,
                "total_rows": total_rows,
                "segment_count": segment_count,
                "source": {
                    "file": "source.jsonl",
                    "bytes": source_size,
                    "sha256": _sha256_file(source_path)[0],
                },
                "segments": segment_meta,
                "created_at": _utcnow(),
            }
            _atomic_write_json(staging / "plan.json", plan)

            state = {
                "schema_version": SCHEMA_VERSION,
                "job_id": job_id,
                "status": "ready",
                "version": version,
                "fingerprint": fingerprint,
                "idempotency_key": idempotency_key,
                "created_at": _utcnow(),
                "updated_at": _utcnow(),
                "segments": {
                    str(meta["index"]): {
                        "status": "pending", "write_count": 0, "written_at": None,
                    }
                    for meta in segment_meta
                },
                "manifest_checksum": None,
                "manifest_ready": False,
                "published": False,
                "delivery_state": "pending",
                "receipt_id": None,
                "superseded_by": None,
                "last_error": None,
            }
            _atomic_write_json(staging / "state.json", state)

            (staging / "segments").mkdir(exist_ok=True)
            (staging / "final").mkdir(exist_ok=True)

            job_dir.parent.mkdir(parents=True, exist_ok=True)
            if job_dir.exists():
                # 并发创建同一个 job_id：保留先到的目录，丢弃本次 staging。
                shutil.rmtree(staging, ignore_errors=True)
            else:
                os.replace(staging, job_dir)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    # ---- 执行与续写 ----

    def run_job(
        self,
        job_id: str,
        *,
        fail_after_segments: int | None = None,
    ) -> dict[str, Any]:
        job_dir = self._job_dir(job_id)
        plan = _read_json(job_dir / "plan.json")
        state = _read_json(job_dir / "state.json")
        if plan is None or state is None:
            raise ExportJobError(f"导出任务 {job_id} 不存在", code="job_not_found")

        effective = self._effective_status(job_id, state)
        if effective == "superseded":
            return self.get_job(job_id)
        if effective == "complete" and all(
            self._segment_is_valid(job_dir, segment)
            for segment in plan.get("segments", [])
        ):
            # 已完成且每个分段校验通过：不重复写入。
            return self.get_job(job_id)

        lock = self._job_lock(job_id)
        if not lock.acquire(blocking=False):
            result = self.get_job(job_id)
            result["busy"] = True
            return result
        try:
            winner = self._completed_sibling(plan)
            if winner is not None and winner != job_id:
                self._mark_superseded(job_id, winner)
                return self.get_job(job_id)
            self._reconcile_state(job_dir, plan, state)
            return self._write_segments_and_commit(
                job_id, job_dir, plan, state,
                fail_after_segments=fail_after_segments,
            )
        finally:
            lock.release()

    def resume_job(self, job_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.run_job(job_id, **kwargs)

    def _write_segments_and_commit(
        self,
        job_id: str,
        job_dir: Path,
        plan: dict[str, Any],
        state: dict[str, Any],
        *,
        fail_after_segments: int | None,
    ) -> dict[str, Any]:
        spec = ExportFormatSpec.from_dict(plan["template"])
        segments = plan.get("segments", [])
        state_segments: dict[str, Any] = state.setdefault("segments", {})

        self._update_state(job_dir, state, status="writing", last_error=None)

        source_digest, source_bytes = _sha256_file(job_dir / plan["source"]["file"])
        if source_digest != plan["source"]["sha256"] or source_bytes != int(plan["source"]["bytes"]):
            self._update_state(
                job_dir, state, status="failed",
                last_error="导出快照校验和不一致，请重新创建导出任务")
            return self.get_job(job_id)

        written = 0
        skipped = 0
        try:
            for segment in segments:
                key = str(segment["index"])
                entry = state_segments.setdefault(
                    key, {"status": "pending", "write_count": 0, "written_at": None})
                if self._segment_is_valid(job_dir, segment):
                    if entry.get("status") != "confirmed":
                        entry["status"] = "confirmed"
                        entry["confirmed_at"] = _utcnow()
                        self._save_state(job_dir, state)
                    skipped += 1
                    continue
                if fail_after_segments is not None and written >= int(fail_after_segments):
                    raise ExportInterrupted(
                        f"模拟中断：已写入 {written} 个分段，剩余分段保持待续写")
                self._write_segment(job_dir, plan, segment)
                entry.update({
                    "status": "confirmed",
                    "write_count": int(entry.get("write_count", 0)) + 1,
                    "written_at": _utcnow(),
                    "confirmed_at": _utcnow(),
                })
                self._save_state(job_dir, state)
                written += 1

            state["last_run"] = {
                "written": written, "skipped": skipped, "at": _utcnow(),
            }

            missing = [
                segment["index"] for segment in segments
                if not self._segment_is_valid(job_dir, segment)
            ]
            if missing:
                raise ExportInterrupted(f"分段未通过校验：{missing}")

            with self._commit_lock:
                winner = self._completed_sibling(plan)
                if winner is not None and winner != job_id:
                    self._mark_superseded(job_id, winner)
                    return self.get_job(job_id)
                manifest = self._assemble_and_commit(job_dir, plan, spec)
                state["manifest_checksum"] = manifest["manifest_checksum"]
                state["manifest_ready"] = True
                state["published"] = False
                state["last_error"] = None
                self._update_state(job_dir, state, status="complete")
                self._publish_completed(job_id, plan, state)
        except ExportInterrupted as exc:
            status = "partial" if self._has_confirmed(state) else "failed"
            self._update_state(job_dir, state, status=status, last_error=str(exc))
        except Exception as exc:  # noqa: BLE001
            status = "partial" if self._has_confirmed(state) else "failed"
            self._update_state(job_dir, state, status=status, last_error=str(exc))
        return self.get_job(job_id)

    def _reconcile_state(
        self, job_dir: Path, plan: dict[str, Any], state: dict[str, Any],
    ) -> None:
        """把磁盘上已存在且校验通过、但状态未确认的分段补记为已完成。"""
        changed = False
        for segment in plan.get("segments", []):
            key = str(segment["index"])
            entry = state.setdefault("segments", {}).setdefault(
                key, {"status": "pending", "write_count": 0, "written_at": None})
            if entry.get("status") != "confirmed" and self._segment_is_valid(job_dir, segment):
                entry["status"] = "confirmed"
                entry["confirmed_at"] = _utcnow()
                changed = True
        if changed:
            self._save_state(job_dir, state)

    def _write_segment(
        self, job_dir: Path, plan: dict[str, Any], segment: dict[str, Any],
    ) -> None:
        spec = ExportFormatSpec.from_dict(plan["template"])
        source_path = job_dir / plan["source"]["file"]
        target = job_dir / segment["file"]
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")

        hasher = hashlib.sha256()
        with open(source_path, "rb") as source, open(tmp, "wb") as output:
            if segment["index"] == 1:
                header_bytes = _encode_header(plan.get("header", ""), spec)
                if header_bytes:
                    hasher.update(header_bytes)
                    output.write(header_bytes)
            source.seek(segment["byte_start"])
            payload = source.read(segment["byte_end"] - segment["byte_start"])
            for raw_line in payload.splitlines():
                if not raw_line:
                    continue
                line = json.loads(raw_line.decode("utf-8"))
                encoded = _encode_line(line + "\n", spec)
                hasher.update(encoded)
                output.write(encoded)
            output.flush()
            os.fsync(output.fileno())

        checksum = hasher.hexdigest()
        if checksum != segment["checksum"]:
            tmp.unlink(missing_ok=True)
            raise ExportInterrupted(
                f"分段 {segment['index']} 校验和不一致，已丢弃临时文件")
        os.replace(tmp, target)

    def _segment_is_valid(self, job_dir: Path, segment: dict[str, Any]) -> bool:
        path = job_dir / segment["file"]
        try:
            stat = path.stat()
            if stat.st_size != int(segment["bytes"]):
                return False
            digest = hashlib.sha256()
            with open(path, "rb") as handle:
                for chunk in iter(lambda: handle.read(65536), b""):
                    digest.update(chunk)
            return digest.hexdigest() == segment["checksum"]
        except OSError:
            return False

    def _assemble_and_commit(
        self,
        job_dir: Path,
        plan: dict[str, Any],
        spec: ExportFormatSpec,
    ) -> dict[str, Any]:
        final_dir = job_dir / "final"
        final_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{plan['job_id']}.{spec.extension}"
        final_path = final_dir / filename
        tmp = final_path.with_name(f".{filename}.{uuid.uuid4().hex}.tmp")

        hasher = hashlib.sha256()
        total_bytes = 0
        with open(tmp, "wb") as output:
            if not plan.get("segments"):
                header_bytes = _encode_header(plan.get("header", ""), spec)
                if header_bytes:
                    hasher.update(header_bytes)
                    output.write(header_bytes)
                    total_bytes += len(header_bytes)
            for segment in plan.get("segments", []):
                segment_path = job_dir / segment["file"]
                with open(segment_path, "rb") as handle:
                    for chunk in iter(lambda: handle.read(65536), b""):
                        hasher.update(chunk)
                        output.write(chunk)
                        total_bytes += len(chunk)
            output.flush()
            os.fsync(output.fileno())
        os.replace(tmp, final_path)

        static_manifest = {
            "schema_version": SCHEMA_VERSION,
            "job_id": plan["job_id"],
            "fingerprint": plan["fingerprint"],
            "version": plan["version"],
            "template": spec.as_dict(),
            "columns": plan["columns"],
            "header": plan.get("header", ""),
            "header_checksum": plan.get("header_checksum"),
            "window": plan["window"],
            "segment_size": plan["segment_size"],
            "total_rows": plan["total_rows"],
            "segment_count": plan["segment_count"],
            "segments": [
                {
                    "index": segment["index"],
                    "row_start": segment["row_start"],
                    "row_end": segment["row_end"],
                    "row_count": segment["row_count"],
                    "checksum": segment["checksum"],
                    "bytes": segment["bytes"],
                    "file": segment["file"],
                }
                for segment in plan.get("segments", [])
            ],
            "final": {
                "file": f"final/{filename}",
                "filename": filename,
                "bytes": total_bytes,
                "checksum": hasher.hexdigest(),
                "rows": plan["total_rows"],
                "media_type": spec.media_type,
            },
            "completed_at": _utcnow(),
            "downloadable": True,
        }
        manifest = dict(static_manifest)
        manifest["manifest_checksum"] = _sha256_bytes(_canonical_json(static_manifest))
        _atomic_write_json(job_dir / "manifest.json", manifest)
        return manifest

    def _completed_sibling(self, plan: dict[str, Any]) -> str | None:
        """同一指纹下已提交完成标记的首个作业（竞争判定的唯一依据）。"""
        fingerprint = plan.get("fingerprint")
        if not fingerprint:
            return None
        candidates = sorted(
            (
                path for path in self.jobs_root.glob(f"EXP-{str(fingerprint)[:10]}-v*")
                if path.is_dir()
            ),
            key=lambda path: (
                path.stat().st_mtime_ns,
                int(path.name.rsplit("-v", 1)[1])
                if path.name.rsplit("-v", 1)[1].isdigit() else 0,
            ),
        )
        for candidate in candidates:
            candidate_plan = _read_json(candidate / "plan.json")
            if not candidate_plan or candidate_plan.get("fingerprint") != fingerprint:
                continue
            if candidate.name == plan.get("job_id"):
                continue
            if self._load_manifest(candidate.name) is not None:
                return candidate.name
        return None

    def _mark_superseded(self, job_id: str, winner: str) -> None:
        job_dir = self._job_dir(job_id)
        state = _read_json(job_dir / "state.json") or {}
        with self._lock:
            self._update_state(
                job_dir, state, status="superseded", last_error=None,
                extra={"superseded_by": winner, "delivery_state": "conflict"},
                endpoint="conflict",
            )
        self._publish_conflict(job_id, winner)

    # ---- 发布 / 回执 ----

    def _publish_completed(
        self, job_id: str, plan: dict[str, Any], state: dict[str, Any],
    ) -> None:
        manifest = self._load_manifest(job_id)
        if manifest is None:
            return
        store.channel.publish(
            topic=EXPORT_TOPIC,
            source="export_jobs",
            payload={
                # 注意：这里故意不携带格式化文件内容，消费方按 job_id 展开。
                "job_id": job_id,
                "version": plan["version"],
                "fingerprint": plan["fingerprint"],
                "manifest_checksum": manifest["manifest_checksum"],
                "manifest_schema": SCHEMA_VERSION,
                "template_id": plan["template"]["template_id"],
                "template_name": plan["template"]["template_name"],
                "total_rows": plan["total_rows"],
                "segment_count": plan["segment_count"],
                "final_filename": manifest["final"]["filename"],
                "formatted_output_included": False,
                "downloadable": True,
                "completed_at": manifest["completed_at"],
            },
        )
        state["published"] = True
        state["published_at"] = _utcnow()
        self._save_state(self._job_dir(job_id), state)

    def _publish_conflict(self, job_id: str, winner: str) -> None:
        store.channel.publish(
            topic=CONFLICT_TOPIC,
            source="export_jobs",
            payload={
                "job_id": job_id,
                "superseded_by": winner,
                "delivery_state": "conflict",
                "published_at": _utcnow(),
            },
        )

    def issue_delivery_receipt(
        self, job_id: str, *, consumer: str = "callback_hook",
    ) -> tuple[dict[str, Any] | None, bool]:
        """为已完成导出签发交付回执；每个任务最多一条，重复调用返回既有回执。"""
        job = self.get_job(job_id)
        if not job.get("downloadable") or not job.get("manifest_ready"):
            return None, False
        if job.get("delivery_state") == "conflict" or job.get("status") == "superseded":
            return None, False
        with self._lock:
            receipts = self._load_receipts()
            for receipt in receipts:
                if receipt.get("job_id") == job_id:
                    return receipt, False
            for receipt in receipts:
                if receipt.get("fingerprint") == job.get("fingerprint"):
                    # 同一指纹只会有一份交付：其他版本（含新建的 vN）不再重复回执。
                    return None, False

            manifest = self._load_manifest(job_id)
            if manifest is None:
                return None, False
            # 交付状态必须是已发布，且清单与发布事件一一对应；
            # 没有完成标记的任务走到这里会提前返回，因此失败文件不会产生回执。
            receipt = {
                "receipt_id": f"RCPT-{uuid.uuid4().hex[:10]}",
                "job_id": job_id,
                "fingerprint": job.get("fingerprint"),
                "version": manifest["version"],
                "template_id": manifest["template"]["template_id"],
                "template_name": manifest["template"]["template_name"],
                "manifest_checksum": manifest["manifest_checksum"],
                "final_filename": manifest["final"]["filename"],
                "final_checksum": manifest["final"]["checksum"],
                "final_bytes": manifest["final"]["bytes"],
                "total_rows": manifest["total_rows"],
                "segment_count": manifest["segment_count"],
                "download_url": f"/api/exports/jobs/{job_id}/download",
                "delivery_state": "delivered",
                "consumer": consumer,
                "delivered_at": _utcnow(),
            }
            receipts.append(receipt)
            _atomic_write_json(self.root / "receipts.json", {"receipts": receipts})

            job_dir = self._job_dir(job_id)
            state = _read_json(job_dir / "state.json") or {}
            state["receipt_id"] = receipt["receipt_id"]
            state["delivery_state"] = "delivered"
            state["delivered_at"] = receipt["delivered_at"]
            self._save_state(job_dir, state)
            return receipt, True

    def list_receipts(self, job_id: str | None = None) -> list[dict[str, Any]]:
        receipts = self._load_receipts()
        if job_id:
            receipts = [r for r in receipts if r.get("job_id") == job_id]
        return receipts

    def _load_receipts(self) -> list[dict[str, Any]]:
        payload = _read_json(self.root / "receipts.json") or {}
        receipts = payload.get("receipts")
        return list(receipts) if isinstance(receipts, list) else []

    # ---- 状态读写 ----

    def _save_state(self, job_dir: Path, state: dict[str, Any]) -> None:
        state["updated_at"] = _utcnow()
        _atomic_write_json(job_dir / "state.json", state)

    def _update_state(
        self,
        job_dir: Path,
        state: dict[str, Any],
        *,
        status: str,
        last_error: str | None = None,
        extra: dict[str, Any] | None = None,
        endpoint: str | None = None,
    ) -> None:
        state["status"] = status
        state["last_error"] = last_error
        if extra:
            state.update(extra)
        self._save_state(job_dir, state)
        if endpoint:
            store.channel.publish(
                topic=endpoint,
                source="export_jobs",
                payload={
                    "job_id": state.get("job_id"),
                    "status": status,
                    "manifest_checksum": state.get("manifest_checksum"),
                    "delivery_state": state.get("delivery_state"),
                    "at": state["updated_at"],
                },
            )

    @staticmethod
    def _has_confirmed(state: dict[str, Any]) -> bool:
        return any(
            entry.get("status") == "confirmed"
            for entry in state.get("segments", {}).values()
        )

    def _effective_status(self, job_id: str, state: dict[str, Any]) -> str:
        status = str(state.get("status") or "ready")
        if status == "superseded":
            return status
        manifest = self._load_manifest(job_id)
        if manifest is not None:
            return "complete"
        return status

    def _load_manifest(self, job_id: str) -> dict[str, Any] | None:
        try:
            job_dir = self._job_dir(job_id)
        except ExportJobError:
            return None
        manifest = _read_json(job_dir / "manifest.json")
        if not manifest:
            return None
        # 版本边界：只接受当前已实现的清单版本；更高版本由更新的代码处理，
        # 旧代码不猜测其语义，直接视为未完成，避免把新格式当成完整文件交付。
        if int(manifest.get("schema_version", 0)) != SCHEMA_VERSION:
            return None
        checksum = manifest.get("manifest_checksum")
        static = {key: value for key, value in manifest.items() if key != "manifest_checksum"}
        if checksum != _sha256_bytes(_canonical_json(static)):
            return None
        if manifest.get("final", {}).get("rows") != manifest.get("total_rows"):
            return None
        if not (job_dir / manifest.get("final", {}).get("file", "")).exists():
            return None
        return manifest

    # ---- 查询 ----

    def get_job(self, job_id: str) -> dict[str, Any]:
        job_dir = self._job_dir(job_id)
        plan = _read_json(job_dir / "plan.json")
        state = _read_json(job_dir / "state.json")
        if plan is None or state is None:
            raise ExportJobError(f"导出任务 {job_id} 不存在", code="job_not_found")
        manifest = self._load_manifest(job_id)
        effective = self._effective_status(job_id, state)

        segments = []
        confirmed = 0
        for segment in plan.get("segments", []):
            entry = state.get("segments", {}).get(str(segment["index"]), {})
            saved = (
                entry.get("status") == "confirmed"
                and (job_dir / segment["file"]).exists()
                and (job_dir / segment["file"]).stat().st_size == int(segment["bytes"])
            )
            if saved:
                confirmed += 1
            segments.append({
                "index": segment["index"],
                "row_start": segment["row_start"],
                "row_end": segment["row_end"],
                "row_count": segment["row_count"],
                "checksum": segment["checksum"],
                "bytes": segment["bytes"],
                "file": segment["file"],
                "status": "confirmed" if saved else "pending",
                "write_count": int(entry.get("write_count", 0)),
                "written_at": entry.get("written_at"),
            })

        total_segments = int(plan.get("segment_count", 0))
        progress = round(confirmed / total_segments, 4) if total_segments else (
            1.0 if effective == "complete" else 0.0
        )
        return {
            "job_id": job_id,
            "status": effective,
            "stored_status": state.get("status"),
            "version": plan.get("version"),
            "fingerprint": plan.get("fingerprint"),
            "template": plan.get("template"),
            "columns": plan.get("columns", []),
            "window": plan.get("window"),
            "segment_size": plan.get("segment_size"),
            "total_rows": plan.get("total_rows"),
            "segment_count": total_segments,
            "confirmed_segments": confirmed,
            "pending_segments": total_segments - confirmed,
            "progress": progress,
            "segments": segments,
            "manifest_ready": manifest is not None,
            "manifest_checksum": (manifest or {}).get("manifest_checksum"),
            "final": (manifest or {}).get("final"),
            "downloadable": manifest is not None and effective == "complete",
            "save_status": "已保存" if manifest is not None else ("部分保存" if confirmed else "未保存"),
            "delivery_state": state.get("delivery_state", "pending"),
            "receipt_id": state.get("receipt_id"),
            "superseded_by": state.get("superseded_by"),
            "last_error": state.get("last_error"),
            "last_run": state.get("last_run"),
            "created_at": plan.get("created_at"),
            "completed_at": (manifest or {}).get("completed_at"),
            "updated_at": state.get("updated_at"),
        }

    def list_jobs(self, *, downloadable_only: bool = False) -> list[dict[str, Any]]:
        jobs = []
        if not self.jobs_root.exists():
            return jobs
        for candidate in self.jobs_root.iterdir():
            if not candidate.is_dir() or not JOB_ID_RE.match(candidate.name):
                continue
            try:
                job = self.get_job(candidate.name)
            except ExportJobError:
                continue
            if downloadable_only and not job["downloadable"]:
                continue
            jobs.append(job)
        jobs.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return jobs

    def download(self, job_id: str) -> tuple[Path, str, str] | None:
        """返回 (文件路径, 下载文件名, media_type)；未完成或校验失败返回 None。"""
        manifest = self._load_manifest(job_id)
        if manifest is None:
            return None
        state = _read_json(self._job_dir(job_id) / "state.json") or {}
        if self._effective_status(job_id, state) == "superseded":
            return None
        final = manifest.get("final") or {}
        path = self._job_dir(job_id) / str(final.get("file", ""))
        try:
            digest = hashlib.sha256()
            with open(path, "rb") as handle:
                for chunk in iter(lambda: handle.read(65536), b""):
                    digest.update(chunk)
            if digest.hexdigest() != final.get("checksum"):
                return None
        except OSError:
            return None
        return path, str(final.get("filename") or path.name), str(final.get("media_type") or "application/octet-stream")

    def stats(self) -> dict[str, Any]:
        jobs = self.list_jobs()
        return {
            "total_jobs": len(jobs),
            "complete_jobs": sum(1 for job in jobs if job["downloadable"]),
            "partial_jobs": sum(1 for job in jobs if job["status"] in ("partial", "writing", "ready")),
            "failed_jobs": sum(1 for job in jobs if job["status"] == "failed"),
            "conflict_jobs": sum(1 for job in jobs if job["status"] == "superseded"),
            "segments_confirmed": sum(job["confirmed_segments"] for job in jobs),
            "receipts": len(self.list_receipts()),
        }


# 全局导出管理器：API、输出格式服务和回调钩子共用同一份磁盘目录。
export_jobs = ExportJobManager()
