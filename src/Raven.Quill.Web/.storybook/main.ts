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
    // No docs addon reads the prop tables react-docgen extracts, and parsing every component for them
    // made a story open about twice as slowly the first time.
    typescript: { reactDocgen: false },
    viteFinal: (viteConfig) =>
        mergeConfig(viteConfig, {
            // Stories get zustand through a wrapper that records every store, so preview.tsx can reset them all.
            resolve: {
                alias: [{ find: /^zustand$/, replacement: fileURLToPath(new URL("./zustand.ts", import.meta.url)) }],
            },
            // Vite compiles a story's modules only when it is first opened, which took up to 6 s for the large
            // pages. Warming up compiles every story in the background right after start instead. Vitest is
            // left out, as it may be asked to run a single story file.
            server: process.env.VITEST ? {} : { warmup: { clientFiles: ["./src/**/*.stories.{ts,tsx}"] } },
        }),
};

export default config;
