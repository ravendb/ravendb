import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
    stories: ["../src/**/*.stories.@(ts|tsx)"],
    addons: ["@storybook/addon-vitest"],
    framework: "@storybook/react-vite",
    // Serve the app's public dir so the MSW worker script (mockServiceWorker.js)
    // is available to stories from the same place other tools would use it.
    staticDirs: ["../public"],
};

export default config;
