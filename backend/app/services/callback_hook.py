"""回调钩子服务 — 消费校验结果触发下游系统回调。

这是新建的跨模块消费服务（不存在于 spec 的独立模块），
是结果校验链路的末端消费者。

数据流:
    channel("rpa.validation_complete") ← result_verify
    → callback_hook 消费 → 模拟下游系统回调（Webhook, MQ, HTTP）
    → channel("rpa.callback_executed")

    channel("rpa.export_completed") ← export_jobs
    → callback_hook 展开完成发布的导出任务（消息里不带格式化文件）
    → 仅在文件清单完整时签发交付回执 → channel("rpa.export_delivery_receipt")

两种处理选择（追加当前文件 / 发布新版本重新汇总）都只向回调暴露完整文件：
未携带 manifest 的导出任务会被跳过，不确认消息，等分段补齐后再交付。
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Any

from app.services.export_jobs import EXPORT_TOPIC, RECEIPT_TOPIC, export_jobs
from app.store import store


class CallbackHookService:
    """回调钩子管理 — 消费校验结果触发下游通知。"""

    SERVICE_NAME = "callback_hook"

    def __init__(self) -> None:
        self._callbacks: list[dict[str, Any]] = []

    def consume_validation_and_callback(self) -> dict[str, Any]:
        """消费校验结果并触发下游回调。

        从 channel("rpa.validation_complete") 获取校验完成事件，
        根据校验结果决定回调策略（全量通知、仅异常、仅通过）。

        Returns:
            {"triggered": int, "successful": int, "details": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.SERVICE_NAME,
            topic="rpa.validation_complete",
        )
        if not msgs:
            return {"triggered": 0, "successful": 0, "details": []}

        details = []
        triggered = 0
        successful = 0

        for msg in msgs:
            validation = msg.payload
            batch_ref = validation.get("batch_ref", "")
            passed = validation.get("passed", 0)
            failed = validation.get("failed", 0)

            # 决定回调策略
            if failed > 0:
                strategy = "alert_with_data"  # 有失败，通知异常
            else:
                strategy = "success_notify"  # 全量通过，发送成功通知

            if passed == 0 and failed == 0:
                strategy = "skip"  # 无数据，跳过

            if strategy == "skip":
                store.channel.confirm(msg.msg_id, self.SERVICE_NAME)
                continue

            # 模拟回调执行
            callback_id = str(uuid.uuid4())[:8]
            callback_result = self._execute_callback(
                callback_id, batch_ref, strategy, validation)

            triggered += 1
            if callback_result["success"]:
                successful += 1

            # 发布回调结果
            store.channel.publish(
                topic="rpa.callback_executed",
                source=self.SERVICE_NAME,
                payload={
                    "callback_id": callback_id,
                    "batch_ref": batch_ref,
                    "strategy": strategy,
                    "success": callback_result["success"],
                    "response_code": callback_result.get("response_code", 200),
                    "duration_ms": callback_result.get("duration_ms", 0),
                },
            )

            store.channel.confirm(msg.msg_id, self.SERVICE_NAME)
            details.append({
                "callback_id": callback_id,
                "batch_ref": batch_ref,
                "strategy": strategy,
                "success": callback_result["success"],
            })

        return {
            "triggered": triggered,
            "successful": successful,
            "failed": triggered - successful,
            "details": details,
        }

    def _execute_callback(
        self,
        callback_id: str,
        batch_ref: str,
        strategy: str,
        validation: dict[str, Any],
    ) -> dict[str, Any]:
        """模拟执行下游系统回调（HTTP Webhook / MQ 消息）。"""
        duration_ms = random.randint(20, 300)

        # 模拟各种回调场景
        endpoints = {
            "alert_with_data": "/api/webhooks/rpa/alert",
            "success_notify": "/api/webhooks/rpa/delivery",
        }
        endpoint = endpoints.get(strategy, "/api/webhooks/rpa/default")

        # 回调节奏控制：5% 失败率
        success = random.random() > 0.05

        response_code = 200 if success else random.choice([500, 503, 408])

        callback_record = {
            "callback_id": callback_id,
            "batch_ref": batch_ref,
            "strategy": strategy,
            "endpoint": endpoint,
            "passed": validation.get("passed", 0),
            "failed": validation.get("failed", 0),
            "success": success,
            "response_code": response_code,
            "duration_ms": duration_ms,
            "executed_at": datetime.now(timezone.utc).isoformat(),
        }
        self._callbacks.append(callback_record)

        return {
            "callback_id": callback_id,
            "success": success,
            "response_code": response_code,
            "duration_ms": duration_ms,
        }

    def list_callbacks(self,
                        batch_ref: str | None = None) -> list[dict[str, Any]]:
        """列出回调记录。"""
        if batch_ref:
            return [c for c in self._callbacks if c["batch_ref"] == batch_ref]
        return list(self._callbacks)

    # ------------------------------------------------------------------
    # 导出交付回执：消费完成发布的导出任务，展开为完整文件后再回执
    # ------------------------------------------------------------------

    def consume_exports_and_deliver(self) -> dict[str, Any]:
        """消费 channel("rpa.export_completed")，按导出任务展开交付。

        事件负载只带 job_id / 清单校验和，**不含格式化输出文件内容**。
        这里按任务约定展开完整文件：只有 manifest 就绪、所有分段确认、
        校验和自洽时才签发一次交付回执；同一个 job 重复投递不会重复回执。
        清单未完成的事件保持未确认，等恢复续写后再次消费。

        Returns:
            {"observed": int, "delivered": int, "deferred": int,
             "conflicts": int, "details": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.SERVICE_NAME,
            topic=EXPORT_TOPIC,
            include_unconfirmed=True,
        )
        details: list[dict[str, Any]] = []
        delivered = 0
        deferred = 0
        conflicts = 0

        for msg in msgs:
            payload = msg.payload or {}
            job_id = str(payload.get("job_id") or "")
            job = self._load_export_job(job_id)
            if job is None:
                # 事件引用了不存在的任务：确认掉，避免反复投递毒消息。
                store.channel.confirm(msg.msg_id, self.SERVICE_NAME)
                details.append({"job_id": job_id, "state": "missing"})
                continue

            if job.get("status") == "superseded" or job.get("delivery_state") == "conflict":
                conflicts += 1
                store.channel.confirm(msg.msg_id, self.SERVICE_NAME)
                details.append({
                    "job_id": job_id,
                    "state": "conflict",
                    "superseded_by": job.get("superseded_by"),
                })
                continue

            if not job.get("downloadable") or not job.get("manifest_ready"):
                # 文件清单不完整：不交付、不确认，保留待续写后再处理。
                deferred += 1
                details.append({
                    "job_id": job_id,
                    "state": "deferred",
                    "confirmed_segments": job.get("confirmed_segments"),
                    "segment_count": job.get("segment_count"),
                })
                continue

            expected = payload.get("manifest_checksum")
            if expected and expected != job.get("manifest_checksum"):
                deferred += 1
                details.append({
                    "job_id": job_id,
                    "state": "manifest_mismatch",
                    "manifest_checksum": job.get("manifest_checksum"),
                })
                continue

            receipt, created = export_jobs.issue_delivery_receipt(
                job_id, consumer=self.SERVICE_NAME)
            if receipt is None:
                deferred += 1
                details.append({"job_id": job_id, "state": "deferred"})
                continue

            if created:
                delivered += 1
                store.channel.publish(
                    topic=RECEIPT_TOPIC,
                    source=self.SERVICE_NAME,
                    payload={
                        "receipt_id": receipt["receipt_id"],
                        "job_id": job_id,
                        "version": receipt["version"],
                        "template_id": receipt["template_id"],
                        "manifest_checksum": receipt["manifest_checksum"],
                        "final_filename": receipt["final_filename"],
                        "final_checksum": receipt["final_checksum"],
                        "total_rows": receipt["total_rows"],
                        "download_url": receipt["download_url"],
                        "delivered_at": receipt["delivered_at"],
                    },
                )
            store.channel.confirm(msg.msg_id, self.SERVICE_NAME)
            details.append({
                "job_id": job_id,
                "state": "delivered" if created else "already_delivered",
                "receipt_id": receipt["receipt_id"],
            })

        return {
            "observed": len(msgs),
            "delivered": delivered,
            "deferred": deferred,
            "conflicts": conflicts,
            "details": details,
        }

    @staticmethod
    def _load_export_job(job_id: str) -> dict[str, Any] | None:
        try:
            return export_jobs.get_job(job_id)
        except Exception:  # noqa: BLE001 - 毒事件按不存在处理
            return None

    def delivery_receipts(self, job_id: str | None = None) -> list[dict[str, Any]]:
        """查询已签发的交付回执。"""
        return export_jobs.list_receipts(job_id)

    def get_callback_stats(self) -> dict[str, Any]:
        """获取回调统计。"""
        total = len(self._callbacks)
        successful = sum(1 for c in self._callbacks if c["success"])
        return {
            "total_callbacks": total,
            "successful": successful,
            "failed": total - successful,
            "delivery_receipts": len(export_jobs.list_receipts()),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        stats = self.get_callback_stats()
        return {
            "module": self.SERVICE_NAME,
            "entity": "回调钩子",
            "artifact": "下游回调记录",
            **stats,
            "callback_events": len(
                store.channel.snapshot(topic="rpa.callback_executed")),
            "export_events": len(store.channel.snapshot(topic=EXPORT_TOPIC)),
            "receipt_events": len(store.channel.snapshot(topic=RECEIPT_TOPIC)),
        }


# 全局回调实例
callback_hook = CallbackHookService()