import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/upload-csv': 'http://localhost:3000',
      '/run-from-form': 'http://localhost:3000',
      '/table-columns': 'http://localhost:3000',
      '/results': 'http://localhost:3000',
      '/final-result': 'http://localhost:3000',
      '/cancel-job': 'http://localhost:3000',
      '/job-status': 'http://localhost:3000',
      '/results-history': 'http://localhost:3000',
      '/run-history': 'http://localhost:3000',
      '/fetch-all-logs': 'http://localhost:3000',
      '/logs/stream': {
        target: 'http://localhost:3000',
        ws: false
      }
    }
  },
  build: {
    outDir: 'dist'
  }
})