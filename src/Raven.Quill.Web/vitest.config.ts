import path from "path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";
import { storybookTest } from "@storybook/addon-vitest/vitest-plugin";

const dirname = path.dirname(fileURLToPath(import.meta.url));

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
                plugins: [storybookTest({ configDir: path.join(dirname, ".storybook") })],
                test: {
                    name: "storybook",
                    browser: {
                        enabled: true,
                        headless: true,
                        // System Chrome avoids the ~115 MB `playwright install chromium`
                        // download locally and in CI (GitHub runners preinstall Chrome).
                        provider: playwright({ launchOptions: { channel: "chrome" } }),
                        instances: [{ browser: "chromium" }],
                    },
                },
            },
        ],
    },
});
