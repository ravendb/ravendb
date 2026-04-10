const path = require("path");
const webpack = require("webpack");
const CircularDependencyPlugin = require("circular-dependency-plugin");
const TerserPlugin = require("terser-webpack-plugin");
const MiniCssExtractPlugin = require("mini-css-extract-plugin");
const CssMinimizerPlugin = require("css-minimizer-webpack-plugin");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const CopyPlugin = require("copy-webpack-plugin");
const { CleanWebpackPlugin } = require("clean-webpack-plugin");
const ZipPlugin = require("zip-webpack-plugin");
const ForkTsCheckerWebpackPlugin = require("fork-ts-checker-webpack-plugin");

module.exports = (_, args) => {
    const isProductionMode = args && args.mode === "production";
    const isWatchMode = args && args.watch;

    console.log(`PROD?: ${isProductionMode}`);

    return {
        mode: isProductionMode ? "production" : "development",
        devtool: isProductionMode ? "source-map" : "eval-cheap-module-source-map",
        entry: {
            main: "./typescript/main.ts",
            styles: "./wwwroot/Content/css/styles.less",
            "styles-blue": "./wwwroot/Content/css/styles-blue.less",
            "styles-light": "./wwwroot/Content/css/styles-light.less",
            "styles-classic": "./wwwroot/Content/css/styles-classic.less",
            "bs5-styles": "./wwwroot/Content/css/bs5-styles.scss",
            "bs5-styles-blue": "./wwwroot/Content/css/bs5-styles-blue.scss",
            "bs5-styles-light": "./wwwroot/Content/css/bs5-styles-light.scss",
            "bs5-styles-classic": "./wwwroot/Content/css/bs5-styles-classic.scss",
            rql_worker: path.resolve(__dirname, "./languageService/src/index.ts"),
        },
        output: {
            path: __dirname + "/wwwroot/dist",
            filename: "assets/[name].js",
            chunkFilename: isProductionMode ? "assets/[name].[contenthash:8].js" : "assets/[name].js",
            publicPath: "/studio/",
        },
        plugins: getPlugins({ isProductionMode, isWatchMode }),
        optimization: {
            minimize: isProductionMode,
            emitOnErrors: false,
            usedExports: true,
            minimizer: [new TerserPlugin(), new CssMinimizerPlugin()],
        },
        watchOptions: {
            ignored: /node_modules/,
        },
        stats: {
            warnings: false, // set this to true for debugging webpack
            timings: true,
            builtAt: true,
            assets: false,
            entrypoints: false,
            cached: false,
        },
        module: {
            rules: [
                {
                    resourceQuery: /raw/,
                    type: "asset/source",
                },
                {
                    test: /\.font\.js$/,
                    use: [
                        {
                            loader: MiniCssExtractPlugin.loader,
                        },
                        {
                            loader: "css-loader",
                            options: {
                                url: false,
                            },
                        },
                    ],
                },
                {
                    test: require.resolve("bootstrap-multiselect/dist/js/bootstrap-multiselect"),
                    use: "imports-loader?type=commonjs&wrapper=window&additionalCode=var%20define=false;",
                },
                {
                    test: /\.css$/i,
                    use: [
                        {
                            loader: MiniCssExtractPlugin.loader,
                        },
                        {
                            loader: "css-loader",
                            options: {
                                url: true,
                            },
                        },
                    ],
                },
                {
                    test: /\.less$/i,
                    use: [
                        {
                            loader: MiniCssExtractPlugin.loader,
                        },
                        {
                            loader: "css-loader",
                            options: {
                                url: true,
                            },
                        },
                        {
                            loader: "less-loader",
                            options: {
                                implementation: require("less"),
                                sourceMap: false,
                            },
                        },
                    ],
                },
                {
                    test: /\.scss$/,
                    use: [
                        {
                            loader: MiniCssExtractPlugin.loader,
                        },
                        {
                            loader: "css-loader",
                            options: {
                                url: true,
                            },
                        },
                        {
                            loader: "resolve-url-loader",
                            options: {},
                        },
                        {
                            loader: "thread-loader",
                        },
                        {
                            loader: "sass-loader",
                            options: {
                                sourceMap: true,
                                sassOptions: {
                                    quietDeps: true,
                                    silenceDeprecations: ["mixed-decls", "color-functions", "global-builtin", "import"],
                                },
                            },
                        },
                    ],
                },
                {
                    test: /\.tsx?$/,
                    use: {
                        loader: "swc-loader",
                    },
                },
                {
                    test: /\.html$/,
                    use: {
                        loader: "html-loader",
                        options: {
                            minimize: {
                                removeComments: false,
                            },
                        },
                    },
                },
                {
                    test: /\.(ttf|eot|woff2?)([?#].*)?$/,
                    type: "asset",
                    generator: {
                        filename: "assets/fonts/[name].[hash:8][ext]",
                    },
                },
                {
                    test: /\.(png|jpg|jpeg|gif|svg|webp|lottie)$/,
                    type: "asset/resource",
                    generator: {
                        filename: "assets/img/[name].[hash:8][ext]",
                    },
                },
            ],
        },
        resolve: {
            modules: [path.resolve(__dirname, "../node_modules"), "node_modules"],
            extensions: [".js", ".ts", ".tsx"],
            fallback: {
                fs: false,
            },
            alias: {
                common: path.resolve(__dirname, "typescript/common"),
                external: path.resolve(__dirname, "typescript/external"),
                models: path.resolve(__dirname, "typescript/models"),
                commands: path.resolve(__dirname, "typescript/commands"),
                durandalPlugins: path.resolve(__dirname, "typescript/durandalPlugins"),
                viewmodels: path.resolve(__dirname, "typescript/viewmodels"),
                components: path.resolve(__dirname, "typescript/components"),
                overrides: path.resolve(__dirname, "typescript/overrides"),
                widgets: path.resolve(__dirname, "typescript/widgets"),
                views: path.resolve(__dirname, "wwwroot/App/views"),
                test: path.resolve(__dirname, "typescript/test"),
                hooks: path.resolve(__dirname, "typescript/components/hooks"),

                endpoints: path.resolve(__dirname, "typings/server/endpoints"),
                configuration: path.resolve(__dirname, "typings/server/configuration"),

                Content: path.resolve(__dirname, "wwwroot/Content/"),
                wwwroot: path.resolve(__dirname, "wwwroot/"),
                d3: path.resolve(__dirname, "wwwroot/Content/custom_d3"),
                qrcodejs: path.resolve(__dirname, "wwwroot/Content/custom_qrcode"),

                Favico: path.resolve(__dirname, "node_modules/favico.js/favico"),
                durandal: path.resolve(__dirname, "node_modules/durandal/js"),
                jquery: path.resolve(__dirname, "node_modules/jquery/dist/jquery"),
                plugins: path.resolve(__dirname, "node_modules/durandal/js/plugins"),
                jwerty: path.resolve(__dirname, "node_modules/jwerty-globals-fixed/jwerty"),
            },
        },
    };
};

function getPlugins({ isProductionMode, isWatchMode }) {
    const plugins = [
        new MiniCssExtractPlugin({
            filename: "styles/[name].css",
            chunkFilename: "styles/[name].css",
        }),
        new HtmlWebpackPlugin({
            template: path.join(__dirname, "wwwroot/index.html"),
            inject: true,
            chunks: ["main"],
        }),
        new CopyPlugin({
            patterns: [
                {
                    from: path.resolve(__dirname, "wwwroot/Content/ace/"),
                    to: "ace",
                },
                {
                    from: path.resolve(__dirname, "wwwroot/icons/"),
                },
                {
                    from: path.resolve(__dirname, "wwwroot/version.txt"),
                },
            ],
        }),
        new webpack.DefinePlugin({
            "window.ravenStudioRelease": isProductionMode,
            "process.env.NODE_DEBUG": false,
        }),
        new webpack.ProvidePlugin({
            ko: "knockout",
            _: "lodash",
            jQuery: "jquery",
            jquery: "jquery",
            $: "jquery",
            Prism: "prismjs",
            QRCode: "qrcodejs",
            "window.jQuery": "jquery",
            "window.ko": "knockout",
        }),
        new webpack.ContextReplacementPlugin(/moment[/\\]locale$/, /(en)$/),
        new ForkTsCheckerWebpackPlugin({
            typescript: {
                configFile: path.resolve(__dirname, "tsconfig.json"),
            },
        }),
    ];

    if (!isWatchMode) {
        // it runs on every file save and it's slow - let's skip it in watch mode
        plugins.push(
            new CircularDependencyPlugin({
                exclude: /node_modules/,
                failOnError: true,
                allowAsyncCycles: false,
                cwd: process.cwd(),
            })
        );
    }

    if (isProductionMode) {
        plugins.unshift(
            new CleanWebpackPlugin({
                verbose: true,
            })
        );

        plugins.push(
            new ZipPlugin({
                filename: "Raven.Studio.zip",
            })
        );
    }

    return plugins;
}
