import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// `npm run dev` proxies the API to a local hub (`agenthub demo --no-browser`).
// `npm run build` writes straight into the Python package so `pip install` ships the UI.
export default defineConfig({
  plugins: [react()],
  server: { proxy: { '/api': 'http://localhost:8000' } },
  build: { outDir: '../backend/agenthub/static', emptyOutDir: true, chunkSizeWarningLimit: 900 },
})
