"""输出格式领域服务 — 管理采集结果的输出格式模板，输出 domain_snapshot。

数据工程核心：数据校验与去重策略执行 — 跨模块数据产物

导出能力：模板不再一次性拼接整份字符串，而是交给
``app.services.export_jobs`` 拆成可续写的分段文件，完成标记落定后
才对外发布完整文件与交付回执。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.export_jobs import ExportJobError, export_jobs
from app.store import store


class OutputFormatService(BaseModuleService):
    MODULE = "output_format"
    ENTITY = "格式模板"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["模板名称", "输出类型", "字段映射", "分隔符", "编码方式", "嵌套展开", "空值表示"]
    REQUIRED_FIELDS = ["模板名称", "输出类型", "字段映射"]
    STATUS_ORDER = ["草稿", "已应用", "已废弃"]
    ACTION_RULES = {"应用模板": "草稿", "废弃模板": "已应用"}
    NEGATIVE_ACTIONS = []

    def format_output(self, template_id: int,
                       records: list[dict[str, Any]]) -> dict[str, Any]:
        """按指定模板格式化输出数据。

        模拟将采集记录按模板配置的输出格式转换。

        Returns:
            {"template_id": int, "formatted_count": int, "format_type": str}
        """
        template = store.find(self.MODULE, template_id)
        if template is None:
            return {"template_id": template_id, "error": "模板不存在"}

        output_type = template.get("输出类型", "csv")
        for record in records:
            record["_format"] = output_type
            record["_template"] = template.get("模板名称")

        return {
            "template_id": template_id,
            "formatted_count": len(records),
            "format_type": output_type,
            "encoding": template.get("编码方式", "utf-8"),
            "null_placeholder": template.get("空值表示", "NA"),
        }

    def list_active_templates(self) -> list[dict[str, Any]]:
        """列出已应用的输出模板。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("模板名称"),
                "type": r.get("输出类型"), "mapping": r.get("字段映射"),
                "delimiter": r.get("分隔符"), "encoding": r.get("编码方式"),
            }
            for r in rows if r.get("status") == "已应用"
        ]

    # ---- 分段导出（断点续写 + 原子发布） ----

    def start_export(
        self,
        template_id: int,
        *,
        batch_ref: str | None = None,
        entry_start: int | None = None,
        entry_end: int | None = None,
        offset: int = 0,
        limit: int | None = None,
        segment_size: int = 200,
        new_version: bool = False,
    ) -> dict[str, Any]:
        """读取采集记录并创建分段导出任务（只落计划与快照，不写全量字符串）。"""
        try:
            return export_jobs.create_job(
                template_id,
                batch_ref=batch_ref,
                entry_start=entry_start,
                entry_end=entry_end,
                offset=offset,
                limit=limit,
                segment_size=segment_size,
                new_version=new_version,
            )
        except ExportJobError as exc:
            return {"error": str(exc), "code": exc.code}

    def run_export(
        self,
        job_id: str,
        *,
        fail_after_segments: int | None = None,
    ) -> dict[str, Any]:
        """写入缺失分段；已确认分段直接跳过，失败恢复不会重新下载已写数据。"""
        try:
            return export_jobs.run_job(
                job_id, fail_after_segments=fail_after_segments)
        except ExportJobError as exc:
            return {"error": str(exc), "code": exc.code}

    def export_progress(self, job_id: str) -> dict[str, Any]:
        """输出工作台使用：分段进度、校验和、保存状态。"""
        try:
            return export_jobs.get_job(job_id)
        except ExportJobError as exc:
            return {"error": str(exc), "code": exc.code}

    def list_exports(self, *, downloadable_only: bool = False) -> dict[str, Any]:
        jobs = export_jobs.list_jobs(downloadable_only=downloadable_only)
        return {"items": jobs, "total": len(jobs), "stats": export_jobs.stats()}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        return {
            **base,
            "active_templates": sum(1 for r in rows if r.get("status") == "已应用"),
            "deprecated_templates": sum(1 for r in rows if r.get("status") == "已废弃"),
            "format_types": list({r.get("输出类型", "") for r in rows if r.get("输出类型")}),
            "export_jobs": export_jobs.stats(),
        }