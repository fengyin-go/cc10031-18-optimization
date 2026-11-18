import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import ExportWorkbench from '@/views/ExportWorkbench.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/workbench', name: 'export-workbench', component: ExportWorkbench },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
