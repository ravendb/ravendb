import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
    stories: ["../src/**/*.stories.@(ts|tsx)"],
    addons: ["@storybook/addon-vitest"],
    framework: "@storybook/react-vite",
    // The MSW worker script lives in .storybook/public (not the app's public dir)
    // so it is served to stories but never copied into the production build.
    //
    // The widget bundle is mapped at /widget so the theme editor's preview iframe resolves the same URL it
    // does in production. Build it first with `pnpm build:widget`; without it the preview frame is empty and
    // the rest of the story still renders.
    staticDirs: ["../public", "./public", { from: "../packages/widget/dist", to: "/widget" }],
};

export default config;
