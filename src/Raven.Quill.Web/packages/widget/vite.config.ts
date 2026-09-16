import path from "node:path";
import { defineConfig } from "vite";
import tailwindcss from "@tailwindcss/vite";

// The widget is served from wwwroot/widget, so every emitted URL has to carry that prefix.
const BASE = "/widget/";

export default defineConfig({
    base: BASE,
    plugins: [tailwindcss()],
    resolve: {
        alias: {
            // preact/compat saves ~56 KB gzip against react-dom while the source stays ordinary React.
            react: "preact/compat",
            "react-dom": "preact/compat",
            "react-dom/client": "preact/compat",
            "react/jsx-runtime": "preact/jsx-runtime",
            "@": path.resolve(import.meta.dirname, "./src"),
        },
    },
    build: {
        // The C# shell reads this manifest to emit the hashed <link>/<script> URLs.
        manifest: true,
        outDir: "dist",
        emptyOutDir: true,
        target: "es2022",
    },
    server: {
        proxy: {
            "/apps": { target: "http://localhost:5000", changeOrigin: true },
        },
    },
});
