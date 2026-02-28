import type { StorybookConfig } from "@storybook/react-webpack5";
import type { Configuration } from "webpack";
import webpackConfigFunc from "../webpack.config.js";
import path from "path";
import webpack from "webpack";
import { hooksForAutoMock } from "../typescript/components/hooks/hooksForAutoMock";

const customHooksAliases: Record<string, string> = Object.fromEntries(
    hooksForAutoMock.map((name: string) => [
        `hooks/${name}`,
        path.resolve(__dirname, "../typescript/components/hooks/__mocks__/" + name),
    ])
);

const webpackConfig: Configuration = webpackConfigFunc(null, {
    mode: "development",
    watch: false,
});

const config: StorybookConfig = {
    framework: "@storybook/react-webpack5",
    core: {
        builder: {
            name: "@storybook/builder-webpack5",
            options: {},
        },
    },
    stories: [
        "../typescript/components/common/**/*.stories.tsx",
        "../typescript/components/pages/**/*.stories.tsx",
        "../typescript/components/shell/**/*.stories.tsx",
        "../typescript/components/setupWizard/**/*.stories.tsx",
    ],
    addons: ["@storybook/addon-a11y", "@storybook/addon-designs"],
    docs: {
        docsMode: false,
    },
    typescript: {
        reactDocgen: false,
    },

    webpackFinal: async (config) => {
        if (config.resolve) {
            config.resolve.alias = {
                ...customHooksAliases,
                ...config.resolve?.alias,
                ...webpackConfig.resolve.alias,
            };
        }

        // Ensure a shared runtime for all entries
        config.optimization ??= {};
        config.optimization.runtimeChunk = "single";

        if (typeof config.entry === "object") {
            // the default style is the last one so it's also initial
            config.entry = {
                ...config.entry,
                "styles-light": path.resolve(__dirname, "../wwwroot/Content/css/styles-light.less"),
                "styles-blue": path.resolve(__dirname, "../wwwroot/Content/css/styles-blue.less"),
                "styles-classic": path.resolve(__dirname, "../wwwroot/Content/css/styles-classic.less"),
                styles: path.resolve(__dirname, "../wwwroot/Content/css/styles.less"),
                "bs5-styles-light": path.resolve(__dirname, "../wwwroot/Content/css/bs5-styles-light.scss"),
                "bs5-styles-blue": path.resolve(__dirname, "../wwwroot/Content/css/bs5-styles-blue.scss"),
                "bs5-styles-classic": path.resolve(__dirname, "../wwwroot/Content/css/bs5-styles-classic.scss"),
                "bs5-styles": path.resolve(__dirname, "../wwwroot/Content/css/bs5-styles.scss"),
            };
        }

        config.watchOptions ??= {};
        config.watchOptions.ignored = /(node_modules|storybook-config-entry|storybook-stories)/;

        const incomingRules = webpackConfig.module.rules.filter(
            (x: any) =>
                (x.use && x.use.indexOf && x.use.indexOf("imports-loader") === 0) ||
                (x.use && x.use.loader === "html-loader") ||
                (x.type && x.type === "asset/source") ||
                (x.test && x.test.toString().includes(".less")) ||
                (x.test && x.test.toString().includes(".font\\.js")) ||
                (x.test && x.test.toString().includes(".scss")) ||
                (x.test && x.test.toString().includes(".tsx"))
        );

        const scssRule = incomingRules.find((x: any) => x.test && x.test.toString().includes(".scss")) as any;
        scssRule.use[0].options = {
            publicPath: "/",
        };

        config.module?.rules?.push(...incomingRules);

        const incomingPluginsNames = [
            "ProvidePlugin",
            "MiniCssExtractPlugin",
            "ForkTsCheckerWebpackPlugin",
            "CopyPlugin",
        ];

        // it runs on every file save and it is very slow (+5s) - let's skip it
        config.plugins = config.plugins.filter(
            (x) =>
                x?.constructor.name !== "WebpackInjectMockerRuntimePlugin" &&
                x?.constructor.name !== "WebpackMockPlugin"
        );

        config.plugins?.push(
            ...incomingPluginsNames.map((name) => webpackConfig.plugins.find((x) => x.constructor.name === name))
        );

        config.plugins?.push(
            new webpack.ProvidePlugin({
                process: "process/browser",
            })
        );

        return config;
    },
};

export default config;
