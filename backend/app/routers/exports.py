"""分段导出接口：创建、续写、分段下载、任务清单与交付回执。

路由层只做参数校验和响应封装，状态流转全部由
``app.services.export_jobs`` 与 ``app.services.callback_hook`` 负责。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from app.services.callback_hook import callback_hook
from app.services.export_jobs import ExportJobError, export_jobs

router = APIRouter(prefix="/api/exports", tags=["分段导出"])


class ExportJobPayload(BaseModel):
    template_id: int
    batch_ref: str | None = None
    entry_start: int | None = None
    entry_end: int | None = None
    offset: int = 0
    limit: int | None = None
    segment_size: int = 200
    new_version: bool = False
    auto_run: bool = True


class RunExportPayload(BaseModel):
    # 仅用于演练失败恢复：写入 N 个分段后中断。生产调用保持默认 None。
    fail_after_segments: int | None = Field(default=None, ge=0)


def _error(exc: ExportJobError) -> JSONResponse:
    status = 404 if exc.code in ("job_not_found", "template_not_found") else 400
    return JSONResponse(status_code=status, content={"ok": False, "error": str(exc), "code": exc.code})


@router.get("/jobs")
def list_export_jobs(
    downloadable_only: bool = Query(default=False),
) -> JSONResponse:
    """导出任务清单；downloadable_only=true 时只返回清单完整、可下载的任务。"""
    jobs = export_jobs.list_jobs(downloadable_only=downloadable_only)
    return JSONResponse(content={
        "items": jobs,
        "total": len(jobs),
        "stats": export_jobs.stats(),
    })


@router.post("/jobs")
def create_export_job(payload: ExportJobPayload) -> JSONResponse:
    """读取采集记录 + 输出模板，生成分段计划并（默认）写入全部分段。"""
    try:
        job = export_jobs.create_job(
            payload.template_id,
            batch_ref=payload.batch_ref,
            entry_start=payload.entry_start,
            entry_end=payload.entry_end,
            offset=payload.offset,
            limit=payload.limit,
            segment_size=payload.segment_size,
            new_version=payload.new_version,
        )
    except ExportJobError as exc:
        return _error(exc)
    if payload.auto_run and not job.get("downloadable"):
        try:
            job = export_jobs.run_job(job["job_id"])
        except ExportJobError as exc:
            return _error(exc)
    return JSONResponse(status_code=201, content={"ok": True, "job": job})


@router.get("/jobs/{job_id}")
def get_export_job(job_id: str) -> JSONResponse:
    """导出进度：分段边界、校验和、保存状态与交付状态。"""
    try:
        job = export_jobs.get_job(job_id)
    except ExportJobError as exc:
        return _error(exc)
    return JSONResponse(content={"ok": True, "job": job})


@router.post("/jobs/{job_id}/run")
def run_export_job(job_id: str, payload: RunExportPayload | None = None) -> JSONResponse:
    """写入缺失分段并提交完成标记；已确认分段不会重复写入。"""
    try:
        job = export_jobs.run_job(
            job_id,
            fail_after_segments=(payload.fail_after_segments if payload else None),
        )
    except ExportJobError as exc:
        return _error(exc)
    return JSONResponse(content={"ok": True, "job": job})


@router.post("/jobs/{job_id}/resume")
def resume_export_job(job_id: str) -> JSONResponse:
    """失败恢复：从最后一个已确认分段之后继续追加，不重写已确认数据。"""
    try:
        job = export_jobs.resume_job(job_id)
    except ExportJobError as exc:
        return _error(exc)
    return JSONResponse(content={"ok": True, "job": job})


@router.get("/jobs/{job_id}/segments/{index}")
def download_export_segment(job_id: str, index: int):
    """下载单个已确认分段；未确认或校验失败的分段不对外提供。"""
    try:
        job = export_jobs.get_job(job_id)
    except ExportJobError as exc:
        return _error(exc)
    segment = next((s for s in job["segments"] if s["index"] == index), None)
    if segment is None:
        return JSONResponse(status_code=404, content={"ok": False, "error": "分段不存在"})
    if segment["status"] != "confirmed":
        return JSONResponse(status_code=409, content={"ok": False, "error": "分段尚未确认"})
    segment_path = export_jobs._job_dir(job_id) / segment["file"]
    if not segment_path.exists():
        return JSONResponse(status_code=404, content={"ok": False, "error": "分段文件不存在"})
    return FileResponse(
        path=segment_path,
        media_type="text/csv; charset=utf-8",
        filename=segment_path.name,
        headers={"X-Segment-Checksum": segment["checksum"]},
    )


@router.get("/jobs/{job_id}/download")
def download_export(job_id: str):
    """下载完整文件。清单未完成、校验失败或已被取代的导出不提供下载。"""
    try:
        result = export_jobs.download(job_id)
    except ExportJobError as exc:
        return _error(exc)
    if result is None:
        return JSONResponse(
            status_code=409,
            content={"ok": False, "error": "导出文件清单未完成或校验失败，暂不可下载"},
        )
    path, filename, media_type = result
    return FileResponse(
        path=Path(path),
        media_type=f"{media_type}; charset=utf-8" if media_type.startswith("text/") or "json" in media_type else media_type,
        filename=filename,
    )


@router.post("/jobs/{job_id}/deliver")
def deliver_export(job_id: str) -> JSONResponse:
    """主动触发交付：等价于回调钩子消费该任务对应的完成发布事件。"""
    try:
        export_jobs.get_job(job_id)
    except ExportJobError as exc:
        return _error(exc)
    result = callback_hook.consume_exports_and_deliver()
    receipts = callback_hook.delivery_receipts(job_id)
    return JSONResponse(content={
        "ok": True,
        "result": result,
        "receipts": receipts,
    })


@router.get("/receipts")
def list_delivery_receipts(job_id: str | None = Query(default=None)) -> JSONResponse:
    """交付回执列表；每个导出任务最多一条回执。"""
    receipts = callback_hook.delivery_receipts(job_id)
    return JSONResponse(content={"items": receipts, "total": len(receipts)})


@router.get("/stats")
def export_stats() -> dict[str, Any]:
    """导出工作台汇总：完成、部分、失败和冲突任务数。"""
    return export_jobs.stats()
