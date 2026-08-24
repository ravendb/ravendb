import path from "path";
import { defineConfig, loadEnv } from "vite";
import react, { reactCompilerPreset } from "@vitejs/plugin-react";
import babel from "@rolldown/plugin-babel";
import tailwindcss from "@tailwindcss/vite";
import svgr from "vite-plugin-svgr";

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
    const env = loadEnv(mode, process.cwd(), "");
    const apiProxyTarget = env.VITE_API_PROXY_TARGET || "http://localhost:5000";

    return {
        plugins: [react(), babel({ presets: [reactCompilerPreset()] }), tailwindcss(), svgr()],
        resolve: {
            alias: {
                "@": path.resolve(__dirname, "./src"),
            },
        },
        server: {
            proxy: {
                "/api": { target: apiProxyTarget, ws: true },
                "/healthz": apiProxyTarget,
                // Public embed page, iframed by the channel widget preview.
                "/embed": apiProxyTarget,
                // The embeddable widget bundle, which the theme editor frames in preview mode. Served from
                // the backend's wwwroot, so `pnpm build:widget` has to have run at least once.
                "/widget": apiProxyTarget,
            },
        },
    };
});
