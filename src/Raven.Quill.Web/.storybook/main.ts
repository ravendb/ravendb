import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
    stories: ["../src/**/*.stories.@(ts|tsx)"],
    addons: ["@storybook/addon-vitest"],
    framework: "@storybook/react-vite",
    // The MSW worker script lives in .storybook/public (not the app's public dir)
    // so it is served to stories but never copied into the production build.
    staticDirs: ["../public", "./public"],
};

export default config;
