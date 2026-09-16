import path from "node:path";
import { defineConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";
import { storybookTest } from "@storybook/addon-vitest/vitest-plugin";

// The test tooling resolves from the workspace root's devDependencies; only the widget's *runtime*
// dependencies are pinned in its own package.json (see the note there).
export default defineConfig({
    test: {
        projects: [
            {
                extends: "vite.config.ts",
                test: {
                    name: "unit",
                    environment: "node",
                    include: ["src/**/*.test.ts"],
                },
            },
            {
                extends: "vite.config.ts",
                plugins: [storybookTest({ configDir: path.join(import.meta.dirname, ".storybook") })],
                test: {
                    name: "storybook",
                    browser: {
                        enabled: true,
                        headless: true,
                        // System Chrome avoids the ~115 MB `playwright install chromium` download, matching
                        // the dashboard's setup.
                        provider: playwright({ launchOptions: { channel: "chrome" } }),
                        instances: [{ browser: "chromium" }],
                    },
                },
            },
        ],
    },
});
