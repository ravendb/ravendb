import { fileURLToPath } from "node:url";
import type { StorybookConfig } from "@storybook/react-vite";
import { mergeConfig, type Plugin } from "vite";

const ZUSTAND_WRAPPER = fileURLToPath(new URL("./zustand.ts", import.meta.url));

// A resolve.alias would also reach dependencies like tunnel-rat, which pins zustand 4.
function zustandWrapperForAppCode(): Plugin {
    return {
        name: "zustand-wrapper-for-app-code",
        enforce: "pre",
        resolveId(source, importer) {
            if (source === "zustand" && importer && !importer.includes("/node_modules/")) {
                return ZUSTAND_WRAPPER;
            }
        },
    };
}

const config: StorybookConfig = {
    stories: ["../src/**/*.stories.@(ts|tsx)"],
    addons: [
        "@storybook/addon-vitest",
        {
            name: "@storybook/addon-mcp",
            options: {
                // The docs toolset only knows the components that have stories, mostly pages, and tells agents to
                // treat anything else as nonexistent and never to read the source. Agents here read the source.
                toolsets: { docs: false },
            },
        },
    ],
    framework: "@storybook/react-vite",
    // Stories that exist only for their play function. The sidebar's tag filter brings them back
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
    typescript: { reactDocgen: false },
    viteFinal: (viteConfig) =>
        mergeConfig(viteConfig, {
            // App code gets zustand through a wrapper that records every store, so preview.tsx can reset them all.
            plugins: [zustandWrapperForAppCode()],
            server: process.env.VITEST
                ? {}
                : {
                      // Vite compiles a story's modules only when it is first opened, which took up to 6 s for the
                      // large pages. Warming up compiles every story in the background right after start instead.
                      // Vitest is left out, as it may be asked to run a single story file.
                      warmup: { clientFiles: ["./.storybook/preview.tsx", "./src/**/*.stories.{ts,tsx}"] },
                      // Prints a story's browser errors and warnings in the Storybook terminal, where agents read
                      // them too. Vite turns it on by itself only when it detects an agent, and each switch
                      // rebuilt its dependency cache. Vitest prints browser logs on its own.
                      forwardConsole: true,
                  },
        }),
};

export default config;
