import path from "path";
import { defineConfig } from "vite";
import tailwindcss from "@tailwindcss/vite";
import { viteSingleFile } from "vite-plugin-singlefile";

// Builds the expired-build notice (expired.html) that the backend's expiry gate answers every request
// with. The gate swallows every path, so there is no second request an asset could arrive on: the notice
// has to be one file. vite-plugin-singlefile folds the JS and CSS back into the HTML.

export default defineConfig({
    plugins: [tailwindcss(), viteSingleFile()],
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "./src"),
        },
    },
    build: {
        // No dynamic imports to preload, so vite's preload polyfill would inline as dead weight.
        modulePreload: false,
        rollupOptions: {
            input: path.resolve(__dirname, "expired.html"),
        },
        // dist is shared with the SPA build, which runs first and empties it.
        emptyOutDir: false,
    },
});
