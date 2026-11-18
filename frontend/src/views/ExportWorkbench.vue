<template>
  <section class="page export-workbench" data-testid="export-workbench">
    <header class="page-head">
      <div>
        <h2>输出工作台</h2>
        <p class="page-desc">
          按输出模板分段导出采集记录；失败恢复只续写缺失分段，文件清单完整后才会生成一次交付回执。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-workbench" type="button" :disabled="loading" @click="reload">
          {{ loading ? '读取中…' : '刷新进度' }}
        </button>
        <button class="btn primary" data-testid="deliver-exports" type="button" :disabled="delivering || !stats.complete_jobs" @click="deliver">
          {{ delivering ? '交付中…' : '触发交付回执' }}
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" data-testid="create-export" @submit.prevent="createJob">
      <label class="filter-item">
        <span>输出模板</span>
        <select v-model.number="form.templateId" data-testid="export-template">
          <option v-for="template in templates" :key="String(template.id)" :value="Number(template.id)">
            {{ template.name }}（{{ template.type }}）
          </option>
        </select>
      </label>
      <label class="filter-item">
        <span>采集批次</span>
        <input v-model="form.batchRef" data-testid="export-batch" placeholder="留空导出全部批次" />
      </label>
      <label class="filter-item">
        <span>分段大小</span>
        <input v-model.number="form.segmentSize" data-testid="export-segment-size" type="number" min="1" max="5000" />
      </label>
      <label class="filter-item inline">
        <input v-model="form.newVersion" data-testid="export-new-version" type="checkbox" />
        <span>重新汇总为新版本</span>
      </label>
      <button class="btn primary" data-testid="submit-export" type="submit" :disabled="creating || !templates.length">
        {{ creating ? '导出中…' : '开始分段导出' }}
      </button>
    </form>

    <p v-if="errorMessage" class="error-text" data-testid="export-error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="export-notice">{{ notice }}</p>

    <h3 class="section-title">导出任务</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>任务</th><th>模板</th><th>版本</th><th>分段进度</th><th>行数</th>
          <th>保存状态</th><th>交付状态</th><th>完成标记</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <template v-for="job in jobs" :key="job.job_id">
          <tr :data-testid="`export-row-${job.job_id}`" :data-status="job.status">
            <td>{{ job.job_id }}</td>
            <td>{{ job.template.template_name }}</td>
            <td>v{{ job.version }}</td>
            <td>
              <div class="progress" :title="`${job.confirmed_segments}/${job.segment_count} 段`">
                <span class="progress-bar" :style="{ width: `${Math.round(job.progress * 100)}%` }"></span>
              </div>
              <small>{{ job.confirmed_segments }}/{{ job.segment_count }} 段</small>
            </td>
            <td>{{ job.total_rows }}</td>
            <td>{{ job.save_status }}</td>
            <td>{{ deliveryLabel(job) }}</td>
            <td class="mono">{{ job.manifest_checksum ? job.manifest_checksum.slice(0, 12) : '—' }}</td>
            <td class="row-actions">
              <button
                v-if="!job.downloadable && (job.status === 'partial' || job.status === 'failed')"
                class="link" type="button" :data-testid="`resume-${job.job_id}`"
                @click="resume(job)"
              >
                续写缺失分段
              </button>
              <button
                v-if="job.downloadable"
                class="link" type="button" :data-testid="`download-${job.job_id}`"
                @click="download(job)"
              >
                下载完整文件
              </button>
              <span v-if="job.status === 'superseded'" class="muted">
                已被 {{ job.superseded_by }} 取代
              </span>
            </td>
          </tr>
          <tr v-if="expanded[job.job_id]" :class="'segment-detail'">
            <td colspan="9">
              <table class="data-table inner">
                <thead>
                  <tr><th>分段</th><th>行区间</th><th>行数</th><th>校验和</th><th>字节</th><th>保存状态</th></tr>
                </thead>
                <tbody>
                  <tr v-for="segment in job.segments" :key="segment.index" :data-testid="`segment-${job.job_id}-${segment.index}`">
                    <td>#{{ segment.index }}</td>
                    <td>{{ segment.row_start }}–{{ segment.row_end }}</td>
                    <td>{{ segment.row_count }}</td>
                    <td class="mono">{{ segment.checksum.slice(0, 16) }}…</td>
                    <td>{{ segment.bytes }}</td>
                    <td>{{ segment.status === 'confirmed' ? '已确认' : '待续写' }}</td>
                  </tr>
                </tbody>
              </table>
            </td>
          </tr>
          <tr v-if="job.segments.length" class="detail-toggle-row">
            <td colspan="9">
              <button class="link" type="button" :data-testid="`toggle-${job.job_id}`" @click="toggle(job.job_id)">
                {{ expanded[job.job_id] ? '收起分段明细' : '查看分段明细' }}
              </button>
              <span v-if="job.last_error" class="muted"> · {{ job.last_error }}</span>
            </td>
          </tr>
        </template>
        <tr v-if="!jobs.length">
          <td colspan="9" class="empty-state">暂无导出任务，选择模板后开始分段导出</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">交付回执</h3>
    <table class="data-table">
      <thead>
        <tr><th>回执号</th><th>导出任务</th><th>文件</th><th>行数</th><th>清单校验和</th><th>交付时间</th></tr>
      </thead>
      <tbody>
        <tr v-for="receipt in receipts" :key="receipt.receipt_id" :data-testid="`receipt-${receipt.job_id}`">
          <td>{{ receipt.receipt_id }}</td>
          <td>{{ receipt.job_id }}</td>
          <td>{{ receipt.final_filename }}</td>
          <td>{{ receipt.total_rows }}</td>
          <td class="mono">{{ receipt.manifest_checksum.slice(0, 12) }}…</td>
          <td>{{ receipt.delivered_at }}</td>
        </tr>
        <tr v-if="!receipts.length">
          <td colspan="6" class="empty-state">尚未生成回执：只有完整文件才会触发交付</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>已完成 {{ stats.complete_jobs }} 个导出，部分/失败 {{ stats.partial_jobs + stats.failed_jobs }} 个</span>
      <span v-if="stats.conflict_jobs">并发冲突 {{ stats.conflict_jobs }} 个（只接受首个成功结果）</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import {
  createExportJob,
  deliverExports,
  downloadExportFile,
  loadExportWorkbench,
  resumeExportJob,
} from '@/api/backend-service'
import type { ExportJob, ExportReceipt, ExportWorkbench } from '@/api/backend-service'
import { listEntries } from '@/api/backend-service'

type TemplateRow = { id: number; name: string; type: string }

const stats = ref<ExportWorkbench['stats']>({
  total_jobs: 0, complete_jobs: 0, partial_jobs: 0, failed_jobs: 0,
  conflict_jobs: 0, segments_confirmed: 0, receipts: 0,
})
const jobs = ref<ExportJob[]>([])
const receipts = ref<ExportReceipt[]>([])
const templates = ref<TemplateRow[]>([])
const expanded = reactive<Record<string, boolean>>({})
const loading = ref(false)
const creating = ref(false)
const delivering = ref(false)
const errorMessage = ref('')
const notice = ref('')
const form = reactive({ templateId: 0, batchRef: '', segmentSize: 200, newVersion: false })

const cards = computed(() => [
  { label: '导出任务', value: stats.value.total_jobs },
  { label: '可下载完整文件', value: stats.value.complete_jobs },
  { label: '部分/失败（待续写）', value: stats.value.partial_jobs + stats.value.failed_jobs },
  { label: '已确认分段', value: stats.value.segments_confirmed },
  { label: '交付回执', value: stats.value.receipts },
])

function deliveryLabel(job: ExportJob): string {
  if (job.status === 'superseded') return '冲突（已被取代）'
  if (job.receipt_id) return `已交付 ${job.receipt_id}`
  if (job.downloadable) return '待交付'
  return job.pending_segments ? `待续写 ${job.pending_segments} 段` : '未交付'
}

function toggle(jobId: string) {
  expanded[jobId] = !expanded[jobId]
}

async function reload() {
  loading.value = true
  errorMessage.value = ''
  try {
    const payload = await loadExportWorkbench()
    stats.value = payload.stats
    jobs.value = [...payload.jobs, ...payload.incomplete]
    receipts.value = payload.receipts
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出工作台读取失败'
  } finally {
    loading.value = false
  }
}

async function loadTemplates() {
  try {
    const payload = await listEntries('output_format', {})
    templates.value = payload.items.map((row) => ({
      id: Number(row.id),
      name: String(row['模板名称'] ?? row.id),
      type: String(row['输出类型'] ?? 'csv'),
    }))
    if (!form.templateId && templates.value.length) {
      form.templateId = templates.value[0].id
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '输出模板读取失败'
  }
}

async function createJob() {
  creating.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const job = await createExportJob({
      template_id: form.templateId,
      batch_ref: form.batchRef.trim() || undefined,
      segment_size: Number(form.segmentSize) || 200,
      new_version: form.newVersion,
    })
    notice.value = job.downloadable
      ? `导出完成：${job.total_rows} 行 / ${job.segment_count} 段，文件清单已提交`
      : `已写入 ${job.confirmed_segments}/${job.segment_count} 段，可在任务行续写`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '创建导出任务失败'
  } finally {
    creating.value = false
  }
}

async function resume(job: ExportJob) {
  errorMessage.value = ''
  try {
    const resumed = await resumeExportJob(job.job_id)
    notice.value = resumed.downloadable
      ? `续写完成：${resumed.job_id} 已提交文件清单`
      : `仍缺 ${resumed.pending_segments} 个分段，可再次续写`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '续写导出任务失败'
  }
}

async function download(job: ExportJob) {
  errorMessage.value = ''
  try {
    await downloadExportFile(job.job_id, job.final?.filename ?? `${job.job_id}.csv`)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下载失败'
  }
}

async function deliver() {
  delivering.value = true
  errorMessage.value = ''
  try {
    const result = await deliverExports()
    notice.value = `本次签发回执 ${result.delivered} 条；清单未完成的任务保持待交付`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交付回执触发失败'
  } finally {
    delivering.value = false
  }
}

onMounted(async () => {
  await Promise.all([reload(), loadTemplates()])
})
</script>

<style scoped>
.export-workbench .filter-item.inline { flex-direction: row; align-items: center; gap: 6px; }
.export-workbench select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; }
.section-title { font-size: 14px; margin: 18px 0 8px; }
.progress { height: 6px; background: #eef2f7; border-radius: 4px; overflow: hidden; min-width: 90px; }
.progress-bar { display: block; height: 100%; background: var(--brand); }
.mono { font-family: ui-monospace, SFMono-Regular, monospace; font-size: 12px; }
.muted { color: var(--muted); font-size: 12px; }
.notice-text { color: #1f6feb; font-size: 12px; }
.segment-detail td, .detail-toggle-row td { background: #fafbfc; }
.data-table.inner { font-size: 12px; }
</style>
