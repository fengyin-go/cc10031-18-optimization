"""分段导出测试：断点续写、分段校验和、完成标记与交付回执。

覆盖：
1. 分段边界与校验和、完成文件行数完整（无半成品行）
2. 中断后续写只写缺失分段，已确认分段不重新下载/重写
3. 完成标记未提交时不可下载、不出现在清单、不产生回执
4. 同一指纹并发导出只接受首个成功结果，冲突改变交付终态
5. 回调钩子只在文件清单完整后签发一次回执
6. 兼容既有版本边界（未知清单版本不按完成处理）
"""
from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.callback_hook import CallbackHookService
from app.services.export_jobs import (
    CONFLICT_TOPIC,
    EXPORT_TOPIC,
    RECEIPT_TOPIC,
    ExportInterrupted,
    ExportJobError,
    export_jobs,
)
from app.store import store


@pytest.fixture()
def manager(tmp_path: Path):
    """每个用例独立的导出目录，避免跨用例串数据。"""
    export_jobs.configure(tmp_path / "exports")
    yield export_jobs
    export_jobs.configure(tmp_path / "exports")


@pytest.fixture()
def seed_records():
    batch_ref = f"BATCH-EXPORT-{uuid.uuid4().hex[:8]}"
    ids = []
    for index in range(7):
        entry = store.collect_records.add(
            batch_ref,
            "data_extract",
            {"序号": index, "名称": f"产品-{index}", "金额": index * 1.5},
        )
        ids.append(entry.entry_id)
    return batch_ref, ids


def _configure_template(
    template_id: int = 1,
    *,
    mapping: dict[str, str] | None = None,
    output_type: str = "csv",
    delimiter: str = ",",
    null_placeholder: str = "NA",
) -> int:
    template = store.find("output_format", template_id)
    template.update({
        "模板名称": "采集结果导出",
        "输出类型": output_type,
        "字段映射": json.dumps(mapping or {"序号": "编号", "名称": "标题", "金额": "金额"},
                            ensure_ascii=False),
        "分隔符": delimiter,
        "编码方式": "utf-8",
        "嵌套展开": "否",
        "空值表示": null_placeholder,
        "status": "已应用",
    })
    return template_id


def _read_final(manager, job_id: str) -> str:
    download = manager.download(job_id)
    assert download is not None
    path, filename, media_type = download
    assert filename.endswith(".csv")
    assert media_type == "text/csv"
    return Path(path).read_text(encoding="utf-8-sig")


# ---------------------------------------------------------------------------
# 1. 分段边界、校验和与完整文件
# ---------------------------------------------------------------------------

def test_segments_partition_rows_and_manifest_is_complete(manager, seed_records):
    batch_ref, _ = seed_records
    template_id = _configure_template()
    job = manager.create_job(template_id, batch_ref=batch_ref, segment_size=3)

    assert job["status"] == "ready"
    assert job["segment_count"] == 3
    assert job["confirmed_segments"] == 0
    assert job["manifest_ready"] is False
    assert job["downloadable"] is False

    # 分段边界连续且不重叠
    spans = [(s["row_start"], s["row_end"]) for s in job["segments"]]
    assert spans == [(0, 3), (3, 6), (6, 7)]
    assert all(len(s["checksum"]) == 64 for s in job["segments"])

    done = manager.run_job(job["job_id"])
    assert done["status"] == "complete"
    assert done["confirmed_segments"] == 3
    assert done["pending_segments"] == 0
    assert done["manifest_ready"] is True
    assert done["downloadable"] is True
    assert done["save_status"] == "已保存"

    body = _read_final(manager, job["job_id"])
    lines = body.splitlines()
    assert lines[0] == "编号,标题,金额"
    assert len(lines) == 1 + 7
    # 每个数据行都完整（三个字段），没有半成品行
    for position, line in enumerate(lines[1:], start=0):
        assert line.count(",") == 2
        assert line.startswith(f"{position},产品-{position},")


def test_empty_window_still_publishes_empty_file(manager):
    template_id = _configure_template()
    job = manager.create_job(template_id, batch_ref=f"MISSING-{uuid.uuid4().hex}")
    assert job["segment_count"] == 0
    done = manager.run_job(job["job_id"])
    assert done["status"] == "complete"
    assert done["downloadable"] is True
    assert _read_final(manager, job["job_id"]) == "编号,标题,金额\n"


def test_window_selects_entry_range_and_limit(manager, seed_records):
    batch_ref, ids = seed_records
    template_id = _configure_template()
    job = manager.create_job(
        template_id,
        batch_ref=batch_ref,
        entry_start=ids[2],
        entry_end=ids[5],
        segment_size=2,
    )
    assert job["total_rows"] == 4  # ids[2]..ids[5] 含两端边界
    done = manager.run_job(job["job_id"])
    lines = _read_final(manager, job["job_id"]).splitlines()
    assert [line.split(",")[0] for line in lines[1:]] == ["2", "3", "4", "5"]
    assert done["window"]["entry_start"] == ids[2]


# ---------------------------------------------------------------------------
# 2. 断点续写：不重写已确认分段
# ---------------------------------------------------------------------------

def test_resume_skips_confirmed_segments(manager, seed_records):
    batch_ref, _ = seed_records
    template_id = _configure_template()
    job = manager.create_job(template_id, batch_ref=batch_ref, segment_size=2)

    partial = manager.run_job(job["job_id"], fail_after_segments=2)
    assert partial["status"] == "partial"
    assert partial["confirmed_segments"] == 2
    assert partial["pending_segments"] == 2  # 7 行 / 每段 2 行 => 4 段
    assert partial["downloadable"] is False
    assert partial["manifest_ready"] is False
    assert partial["save_status"] == "部分保存"
    first_writes = [s["write_count"] for s in partial["segments"]]
    assert first_writes == [1, 1, 0, 0]

    resumed = manager.resume_job(job["job_id"])
    assert resumed["status"] == "complete"
    assert resumed["downloadable"] is True
    # 已确认分段保持原写入次数，说明没有被重新下载/重写
    assert [s["write_count"] for s in resumed["segments"]] == [1, 1, 1, 1]

    body = _read_final(manager, job["job_id"])
    assert len(body.splitlines()) == 1 + 7


def test_damaged_segment_is_rewritten_and_others_kept(manager, seed_records):
    batch_ref, _ = seed_records
    template_id = _configure_template()
    job = manager.create_job(template_id, batch_ref=batch_ref, segment_size=2)
    manager.run_job(job["job_id"])

    job_dir = manager._job_dir(job["job_id"])
    damaged = job_dir / job["segments"][1]["file"]
    damaged.write_bytes(b"broken\n")

    after = manager.run_job(job["job_id"])
    assert after["status"] == "complete"
    assert [s["write_count"] for s in after["segments"]] == [1, 2, 1, 1]
    assert len(_read_final(manager, job["job_id"]).splitlines()) == 1 + 7


def test_interrupted_job_is_not_downloadable(manager, seed_records):
    batch_ref, _ = seed_records
    template_id = _configure_template()
    job = manager.create_job(template_id, batch_ref=batch_ref, segment_size=2)
    manager.run_job(job["job_id"], fail_after_segments=1)

    downloadable = manager.list_jobs(downloadable_only=True)
    assert job["job_id"] not in {item["job_id"] for item in downloadable}
    assert manager.download(job["job_id"]) is None
    receipt, created = manager.issue_delivery_receipt(job["job_id"])
    assert receipt is None and created is False


# ---------------------------------------------------------------------------
# 3. 交付回执：清单完整后只触发一次
# ---------------------------------------------------------------------------

def test_callback_receipt_only_after_manifest_is_complete(manager, seed_records):
    batch_ref, _ = seed_records
    template_id = _configure_template()
    hook = CallbackHookService()

    job = manager.create_job(template_id, batch_ref=batch_ref, segment_size=2)
    manager.run_job(job["job_id"], fail_after_segments=1)
    # 未完成时没有完成发布事件可消费
    assert hook.consume_exports_and_deliver()["delivered"] == 0

    messages = store.channel.snapshot(topic=EXPORT_TOPIC)
    completed = [m for m in messages if m["payload"].get("job_id") == job["job_id"]]
    assert completed == []

    manager.resume_job(job["job_id"])
    completed = [m for m in store.channel.snapshot(topic=EXPORT_TOPIC)
                 if m["payload"].get("job_id") == job["job_id"]]
    assert len(completed) == 1
    payload = completed[0]["payload"]
    # 完成发布只带清单引用，不携带格式化文件内容
    assert payload["formatted_output_included"] is False
    assert payload["manifest_checksum"]
    assert "rows" not in payload and "content" not in payload

    first = hook.consume_exports_and_deliver()
    assert first["delivered"] >= 1
    receipts = hook.delivery_receipts(job["job_id"])
    assert len(receipts) == 1
    assert receipts[0]["download_url"].endswith(f"/api/exports/jobs/{job['job_id']}/download")

    second = hook.consume_exports_and_deliver()
    assert second["delivered"] == 0
    assert len(hook.delivery_receipts(job["job_id"])) == 1
    receipt_events = [m for m in store.channel.snapshot(topic=RECEIPT_TOPIC)
                      if m["payload"].get("job_id") == job["job_id"]]
    assert len(receipt_events) == 1


# ---------------------------------------------------------------------------
# 4. 冲突：同一指纹只接受首个成功结果
# ---------------------------------------------------------------------------

def test_conflict_only_first_success_is_delivered(manager, seed_records):
    batch_ref, _ = seed_records
    template_id = _configure_template()
    first = manager.create_job(template_id, batch_ref=batch_ref, segment_size=2)
    second = manager.create_job(template_id, batch_ref=batch_ref, segment_size=2,
                                new_version=True)
    assert first["job_id"] != second["job_id"]

    winner = manager.run_job(second["job_id"])
    assert winner["status"] == "complete"

    loser = manager.run_job(first["job_id"])
    assert loser["status"] == "superseded"
    assert loser["delivery_state"] == "conflict"
    assert loser["superseded_by"] == second["job_id"]
    assert loser["downloadable"] is False
    assert "manifest_ready" in loser and loser["manifest_ready"] is False

    receipt, created = manager.issue_delivery_receipt(first["job_id"])
    assert receipt is None and created is False

    # 冲突事件改变交付终态，且不会为落败版本产生回执
    conflicts = [m for m in store.channel.snapshot(topic=CONFLICT_TOPIC)
                 if m["payload"].get("job_id") == first["job_id"]]
    assert len(conflicts) == 1
    assert conflicts[0]["payload"]["delivery_state"] == "conflict"
    assert len(manager.list_receipts(first["job_id"])) == 0

    # 落败版本即使再续写也不会复活
    again = manager.resume_job(first["job_id"])
    assert again["status"] == "superseded"
    assert manager.download(first["job_id"]) is None

    # 同一窗口再次发起导出直接复用首个成功版本
    reused = manager.create_job(template_id, batch_ref=batch_ref, segment_size=2)
    assert reused["job_id"] == second["job_id"]
    assert reused.get("reused_existing") is True


# ---------------------------------------------------------------------------
# 5. 版本边界与参数校验
# ---------------------------------------------------------------------------

def test_unknown_manifest_version_is_not_treated_as_complete(manager, seed_records):
    batch_ref, _ = seed_records
    template_id = _configure_template()
    job = manager.create_job(template_id, batch_ref=batch_ref, segment_size=3)
    manager.run_job(job["job_id"])

    manifest_path = manager._job_dir(job["job_id"]) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = 999
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    reloaded = manager.get_job(job["job_id"])
    assert reloaded["manifest_ready"] is False
    assert reloaded["downloadable"] is False
    assert manager.download(job["job_id"]) is None


def test_invalid_requests_raise_job_errors(manager, seed_records):
    batch_ref, _ = seed_records
    _configure_template()
    with pytest.raises(ExportJobError) as missing:
        manager.create_job(9999)
    assert missing.value.code == "template_not_found"

    with pytest.raises(ExportJobError) as size:
        manager.create_job(1, batch_ref=batch_ref, segment_size=0)
    assert size.value.code == "invalid_segment_size"

    with pytest.raises(ExportJobError) as unknown:
        manager.get_job("../../etc/passwd")
    assert unknown.value.code == "job_not_found"


# ---------------------------------------------------------------------------
# 6. 格式化输出：字段映射、空值与分隔符
# ---------------------------------------------------------------------------

def test_field_mapping_null_placeholder_and_formula_guard(manager):
    batch_ref = f"MAP-{uuid.uuid4().hex[:8]}"
    store.collect_records.add(batch_ref, "data_extract",
                              {"序号": 1, "名称": "=SUM(A1)", "金额": None})
    store.collect_records.add(batch_ref, "data_extract",
                              {"序号": 2, "名称": '带,逗号"引号', "金额": 9.5})
    template_id = _configure_template(
        mapping={"序号": "编号", "名称": "标题", "不存在的字段": "备注"},
        null_placeholder="NULL",
    )
    job = manager.create_job(template_id, batch_ref=batch_ref, segment_size=10)
    done = manager.run_job(job["job_id"])
    assert done["template"]["mapping"]["不存在的字段"] == "备注"

    lines = _read_final(manager, job["job_id"]).splitlines()
    assert lines[0] == "编号,标题,备注"
    assert lines[1] == "1,'=SUM(A1),NULL"
    assert lines[2] == '2,"带,逗号""引号",NULL'


def test_jsonl_output_has_one_complete_record_per_line(manager, seed_records):
    batch_ref, _ = seed_records
    template_id = _configure_template(output_type="jsonl")
    job = manager.create_job(template_id, batch_ref=batch_ref, segment_size=3)
    manager.run_job(job["job_id"])

    download = manager.download(job["job_id"])
    assert download is not None
    path, filename, media_type = download
    assert filename.endswith(".jsonl")
    assert media_type == "application/x-ndjson"
    rows = [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 7
    assert all(set(row) == {"编号", "标题", "金额"} for row in rows)
