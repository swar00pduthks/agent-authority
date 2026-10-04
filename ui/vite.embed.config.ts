import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Builds the <agent-authority> web component (micro-frontend) into the Python package,
// served by the API at /embed/agent-authority.js. Run after the main build (it must not empty outDir).
export default defineConfig({
  plugins: [react()],
  define: { 'process.env.NODE_ENV': JSON.stringify('production') },
  build: {
    outDir: '../backend/agent_authority/static/embed',
    emptyOutDir: true,
    cssCodeSplit: false,
    lib: { entry: 'src/embed.tsx', formats: ['es'], fileName: () => 'agent-authority.js', cssFileName: 'agent-authority' },
    chunkSizeWarningLimit: 1200,
  },
})
