import type { StorybookConfig } from "@storybook/react-vite";

// Storybook itself comes from the workspace root's devDependencies rather than this package's, so the
// widget's *shipped* dependency set stays independent while its stories still run on the real runtime:
// Storybook picks up ../vite.config.ts, so the preact/compat aliases and Tailwind apply here too.
const config: StorybookConfig = {
    stories: ["../src/**/*.stories.@(ts|tsx)"],
    addons: ["@storybook/addon-vitest"],
    framework: "@storybook/react-vite",
};

export default config;
