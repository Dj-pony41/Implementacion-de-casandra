import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  json: {
    // Para importaciones JSON tipo ESM
    namedExports: true,
    stringify: false
  }
});