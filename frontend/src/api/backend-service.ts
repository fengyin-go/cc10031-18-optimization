import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

const API_BASE = import.meta.env.VITE_API_BASE ?? ''

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

// ---- 分段导出：断点续写、校验和与交付回执 ----

export type ExportSegment = {
  index: number
  row_start: number
  row_end: number
  row_count: number
  checksum: string
  bytes: number
  file: string
  status: 'confirmed' | 'pending'
  write_count: number
  written_at: string | null
}

export type ExportJob = {
  job_id: string
  status: string
  version: number
  fingerprint: string
  template: { template_id: number; template_name: string; output_type: string; encoding: string }
  columns: string[]
  window: Record<string, string | number | null>
  segment_size: number
  total_rows: number
  segment_count: number
  confirmed_segments: number
  pending_segments: number
  progress: number
  segments: ExportSegment[]
  manifest_ready: boolean
  manifest_checksum: string | null
  final: { filename: string; rows: number; bytes: number; checksum: string } | null
  downloadable: boolean
  save_status: string
  delivery_state: string
  receipt_id: string | null
  superseded_by: string | null
  last_error: string | null
  created_at: string
  completed_at: string | null
}

export type ExportReceipt = {
  receipt_id: string
  job_id: string
  version: number
  template_name: string
  manifest_checksum: string
  final_filename: string
  final_checksum: string
  total_rows: number
  download_url: string
  delivery_state: string
  delivered_at: string
}

export type ExportWorkbench = {
  stats: {
    total_jobs: number
    complete_jobs: number
    partial_jobs: number
    failed_jobs: number
    conflict_jobs: number
    segments_confirmed: number
    receipts: number
  }
  jobs: ExportJob[]
  incomplete: ExportJob[]
  receipts: ExportReceipt[]
}

async function jsonOrThrow<T>(response: Response, fallback: string): Promise<T> {
  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    const message = payload && typeof payload === 'object' && 'error' in payload
      ? String((payload as { error: unknown }).error)
      : fallback
    throw new Error(message)
  }
  return payload as T
}

export async function loadExportWorkbench(): Promise<ExportWorkbench> {
  return jsonOrThrow<ExportWorkbench>(
    await request('/api/export-workbench'), '导出工作台读取失败')
}

export async function createExportJob(values: {
  template_id: number
  batch_ref?: string
  segment_size?: number
  new_version?: boolean
}): Promise<ExportJob> {
  const payload = await jsonOrThrow<{ job: ExportJob }>(
    await request('/api/exports/jobs', {
      method: 'POST',
      body: JSON.stringify({ ...values, auto_run: true }),
    }),
    '创建导出任务失败',
  )
  return payload.job
}

/** 写入缺失分段并提交完成标记（已确认分段不会重复写入）。 */
export async function resumeExportJob(jobId: string): Promise<ExportJob> {
  const payload = await jsonOrThrow<{ job: ExportJob }>(
    await request(`/api/exports/jobs/${jobId}/resume`, { method: 'POST' }),
    '续写导出任务失败',
  )
  return payload.job
}

/** 让回调钩子消费完成事件：清单完整的任务各签发一次交付回执。 */
export async function deliverExports(): Promise<{ delivered: number; deferred: number }> {
  return jsonOrThrow(
    await request('/api/export-workbench/consume', { method: 'POST' }),
    '交付回执触发失败',
  )
}

/** 完整文件下载地址；半成品导出后端会拒绝并返回错误。 */
export function exportDownloadUrl(jobId: string): string {
  return `${API_BASE}/api/exports/jobs/${jobId}/download`
}

export async function downloadExportFile(jobId: string, filename: string): Promise<void> {
  const response = await request(`/api/exports/jobs/${jobId}/download`)
  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as { error?: string } | null
    throw new Error(payload?.error ?? '导出文件清单未完成，暂不可下载')
  }
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}
