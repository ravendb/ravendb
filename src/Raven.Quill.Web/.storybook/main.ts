import { fileURLToPath } from "node:url";
import type { StorybookConfig } from "@storybook/react-vite";
import { mergeConfig } from "vite";

const config: StorybookConfig = {
    stories: ["../src/**/*.stories.@(ts|tsx)"],
    addons: ["@storybook/addon-vitest"],
    framework: "@storybook/react-vite",
    // Stories that exist only for their play function. Unlike `!dev`, which removes them from the
    // sidebar for good, this only hides them by default: the sidebar's tag filter brings them back,
    // so a failing test can still be opened and watched.
    tags: {
        "test-only": { defaultFilterSelection: "exclude" },
    },
    // The MSW worker script lives in .storybook/public (not the app's public dir)
    // so it is served to stories but never copied into the production build.
    //
    // The widget bundle is mapped at /widget so the theme editor's preview iframe resolves the same URL it
    // does in production. The `storybook` scripts build it first; after editing widget sources mid-session,
    // run `pnpm build:widget` again or the frame keeps serving the stale bundle.
    staticDirs: ["../public", "./public", { from: "../packages/widget/dist", to: "/widget" }],
    // Stories get zustand through a wrapper that records every store, so preview.tsx can reset them all.
    viteFinal: (viteConfig) =>
        mergeConfig(viteConfig, {
            resolve: {
                alias: [{ find: /^zustand$/, replacement: fileURLToPath(new URL("./zustand.ts", import.meta.url)) }],
            },
        }),
};

export default config;
