import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
    stories: ["../src/**/*.stories.@(ts|tsx)"],
    addons: ["@storybook/addon-vitest"],
    framework: "@storybook/react-vite",
    // The MSW worker script lives in .storybook/public (not the app's public dir)
    // so it is served to stories but never copied into the production build.
    //
    // The widget bundle is mapped at /widget so the theme editor's preview iframe resolves the same URL it
    // does in production. The `storybook` scripts build it first; after editing widget sources mid-session,
    // run `pnpm build:widget` again or the frame keeps serving the stale bundle.
    staticDirs: ["../public", "./public", { from: "../packages/widget/dist", to: "/widget" }],
};

export default config;
