/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_GRAFANA_URL?: string;
  readonly VITE_GRAFANA_PORT?: string;
  readonly VITE_PROMETHEUS_URL?: string;
  readonly VITE_PROMETHEUS_PORT?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
