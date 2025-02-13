import type { StorybookConfig } from "@storybook/react-webpack5";
const webpackConfigFunc = require("../webpack.config");
const path = require("path");
const CopyPlugin = require("copy-webpack-plugin");
const ForkTsCheckerWebpackPlugin = require("fork-ts-checker-webpack-plugin");

const webpackConfig = webpackConfigFunc(null, {
    mode: "development",
    watch: false,
});

const customHooksToMock = require("../typescript/components/hooks/hooksForAutoMock.json").hooks;
const customHooksAliases = {};

customHooksToMock.forEach((name: string) => {
    customHooksAliases["hooks/" + name] = path.resolve(__dirname, "../typescript/components/hooks/__mocks__/" + name);
});

const config: StorybookConfig = {
    docs: {
        docsMode: false,
    },
    typescript: {
        reactDocgen: false,
    },
    stories: ["../typescript/**/*.stories.tsx"],
    addons: [
        "@storybook/addon-links",
        "@storybook/addon-essentials",
        "@storybook/addon-interactions",
        "@storybook/addon-webpack5-compiler-swc",
        "@storybook/addon-a11y",
        "@storybook/addon-designs",
    ],

    framework: {
        name: "@storybook/react-webpack5",
        options: {},
    },

    webpackFinal: async (config) => {
        if (config.resolve) {
            config.resolve.alias = {
                ...customHooksAliases,
                ...config.resolve?.alias,
                ...webpackConfig.resolve.alias,
            };
        }

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

        config.plugins?.unshift(webpackConfig.plugins.find((x) => x.constructor.name === "ProvidePlugin"));

        const incomingRules = webpackConfig.module.rules.filter(
            (x) =>
                (x.use && x.use.indexOf && x.use.indexOf("imports-loader") === 0) ||
                (x.use && x.use.loader === "html-loader") ||
                (x.type && x.type === "asset/source") ||
                (x.test && x.test.toString().includes(".less")) ||
                (x.test && x.test.toString().includes(".font\\.js")) ||
                (x.test && x.test.toString().includes(".scss"))
        );

        const scssRule = incomingRules.find((x) => x.test && x.test.toString().includes(".scss"));
        scssRule.use[0].options = {
            publicPath: "/",
        };

        config.plugins?.push(webpackConfig.plugins[0]); // MiniCssExtractPlugin

        const copyPlugin = new CopyPlugin({
            patterns: [
                {
                    from: path.resolve(__dirname, "../wwwroot/Content/ace/"),
                    to: "./ace/",
                },
            ],
        });

        config.plugins?.push(copyPlugin);
        config.plugins?.push(
            new ForkTsCheckerWebpackPlugin({
                typescript: {
                    configFile: path.resolve(__dirname, "../tsconfig.json"),
                },
            })
        );

        config.module?.rules?.push(...incomingRules);

        return config;
    },
};
export default config;
