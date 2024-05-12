const path = require("path");
const webpack = require('webpack');
const CircularDependencyPlugin = require("circular-dependency-plugin");
const TerserPlugin = require("terser-webpack-plugin");
const MiniCssExtractPlugin = require("mini-css-extract-plugin");
const CssMinimizerPlugin = require("css-minimizer-webpack-plugin");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const CopyPlugin = require("copy-webpack-plugin");
const { CleanWebpackPlugin } = require('clean-webpack-plugin');
const ZipPlugin = require("zip-webpack-plugin");
const ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin');

// const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;

module.exports = (env, args) => {
    const isProductionMode = args && args.mode === 'production';

    console.log(`PROD?: ${isProductionMode}`);

    const cleanPlugin = new CleanWebpackPlugin({
        verbose: true
    });

    const htmlPlugin = new HtmlWebpackPlugin({
        template: path.join(__dirname, 'wwwroot/index.html'),
        inject: true,
        chunks: ["main"]
    });

    const copyPlugin = new CopyPlugin({
        patterns: [
            {
                from: path.resolve(__dirname, 'wwwroot/Content/ace/'),
                to: "ace"
            },
            {
                from: path.resolve(__dirname, "wwwroot/icons/"),
            },
            {
                from: path.resolve(__dirname, "wwwroot/version.txt"),
            }
        ]
    });

    const miniCssExtractPlugin = new MiniCssExtractPlugin({
        filename: "styles/[name].css",
        chunkFilename: "styles/[name].css"
    });
    
    const plugins = [
        miniCssExtractPlugin,
        htmlPlugin,
        copyPlugin,
        new CircularDependencyPlugin({
            // exclude detection of files based on a RegExp
            exclude: /node_modules/,
            failOnError: true,
            allowAsyncCycles: false,
            // set the current working directory for displaying module paths
            cwd: process.cwd(),
        }),
        new webpack.DefinePlugin({
            "window.ravenStudioRelease": isProductionMode,
            'process.env.NODE_DEBUG': false
        }),
        new webpack.ProvidePlugin({
            ko: "knockout",
            "_": "lodash",
            "jQuery": "jquery",
            "jquery": "jquery",
            "$": "jquery",
            "Prism": "prismjs",
            "QRCode": "qrcodejs",
            'window.jQuery': 'jquery',
            'window.ko': "knockout"
        }),
        new webpack.ContextReplacementPlugin(/moment[\/\\]locale$/, /(en)$/),
        new ForkTsCheckerWebpackPlugin({
            typescript: {
                configFile: path.resolve(__dirname, "tsconfig.json")
            }
        }),
        //new BundleAnalyzerPlugin()
    ];
    
    if (isProductionMode) {
        plugins.unshift(cleanPlugin);
        plugins.push(new ZipPlugin({
            filename: "Raven.Studio.zip"
        }));
    }
    
    return {
        mode: isProductionMode ? "production" : "development",
        devtool: isProductionMode ? "source-map" : "inline-source-map",
        entry: { 
            main: "./typescript/main.ts",
            "styles": "./wwwroot/Content/css/styles.less",
            "styles-blue": "./wwwroot/Content/css/styles-blue.less",
            "styles-light": "./wwwroot/Content/css/styles-light.less",
            "styles-classic": "./wwwroot/Content/css/styles-classic.less",
            "bs5-styles": "./wwwroot/Content/css/bs5-styles.scss",
            "bs5-styles-blue": "./wwwroot/Content/css/bs5-styles-blue.scss",
            "bs5-styles-light": "./wwwroot/Content/css/bs5-styles-light.scss",
            "bs5-styles-classic": "./wwwroot/Content/css/bs5-styles-classic.scss",
            "rql_worker": path.resolve(__dirname, './languageService/src/index.ts')
        },
        output: {
            path: __dirname + '/wwwroot/dist',
            filename: 'assets/[name].js',
            chunkFilename: isProductionMode ? "assets/[name].[contenthash:8].js" : "assets/[name].js",
            publicPath: "/studio/"
        },
        plugins: plugins,
        optimization: {
            minimize: isProductionMode,
            emitOnErrors: false,
            usedExports: true,
            minimizer: [
                new TerserPlugin(),
                new CssMinimizerPlugin()
            ]
        },
        module: {
            rules: [
                {
                    resourceQuery: /raw/,
                    type: 'asset/source',
                },
                {
                    test: /\.font\.js$/,
                    use: [
                        MiniCssExtractPlugin.loader,
                        {
                            loader: 'css-loader',
                            options: {
                                url: false
                            }
                        },
                        {
                            loader: 'webfonts-loader',
                            options: {
                                codepoints: {
                                    lock: 0xf101,
                                    "node-leader": 0xf102,
                                    placeholder: 0xf103,
                                    "dbgroup-member": 0xf104,
                                    "dbgroup-promotable": 0xf105,
                                    "dbgroup-rehab": 0xf106,
                                    "server-wide-backup": 0xf107,
                                    backup2: 0xf108,
                                    "ravendb-etl": 0xf109,
                                    "external-replication": 0xf10A,
                                    "sql-etl": 0xf10B,
                                    "olap-etl": 0xf10C,
                                    "elastic-search-etl": 0xf10D,
                                    "subscription": 0xf10E,
                                    "pull-replication-hub": 0xf10F,
                                    "pull-replication-agent": 0xf110,
                                    "copy-to-clipboard": 0xf111,
                                    "unfold": 0xf112,
                                    "database": 0xf113,
                                    "arrow-down": 0xf114,
                                    "arrow-right": 0xf115,
                                    "edit": 0xf116,
                                    "cancel": 0xf117,
                                    "warning": 0xf118,
                                    "default": 0xf119,
                                    "server": 0xf11A,
                                    "check": 0xf11B,
                                    "document": 0xf11C,
                                    "trash": 0xf11D,
                                    "info": 0xf11E,
                                    "danger": 0xf11F,
                                    "connection-lost": 0xf120,
                                    "empty-set": 0xf121,
                                    "disabled": 0xf122,
                                    "conflicts": 0xf123,
                                    "waiting": 0xf124,
                                    "cluster-member": 0xf125,
                                    "cluster-promotable": 0xf126,
                                    "cluster-watcher": 0xf127,
                                    "arrow-up": 0xf128,
                                    "kafka-etl": 0xf129,
                                    "rabbitmq-etl": 0xf130,
                                    "kafka-sink": 0xf131,
                                    "rabbitmq-sink": 0xf132,
                                    "preview": 0xf133,
                                    "azure-queue-storage-etl": 0xf134,
                                    "corax-include-null-match": 0xf140,
                                    "corax-fallback": 0xf141,
                                    "corax-all-entries-match": 0xf142,
                                    "corax-boosting-match": 0xf143,
                                    "corax-forward": 0xf144,
                                    "corax-memoization-match": 0xf145,
                                    "corax-multi-term-match": 0xf146,
                                    "corax-operator-and": 0xf147,
                                    "corax-operator-andnot": 0xf148,
                                    "corax-operator-or": 0xf149,
                                    "corax-phrase-query": 0xf14A,
                                    "corax-sorting-match": 0xf14B,
                                    "corax-spatial-match": 0xf14C,
                                    "corax-term-match": 0xf14D,
                                    "corax-unary-match": 0xf14E,
                                    "corax-backward": 0xf14F,
                                    "corax-sort-az": 0xf150,
                                    "corax-sort-za": 0xf151,
                                    "close": 0xf162,
                                },
                                cssTemplate: path.resolve(__dirname, "wwwroot/Content/css/fonts/icomoon.template.css.hbs")
                            }
                        }
                    ]
                },
                {
                    test: require.resolve('bootstrap-multiselect/dist/js/bootstrap-multiselect'),
                    use: 'imports-loader?type=commonjs&wrapper=window&additionalCode=var%20define=false;',
                },
                {
                    test: /\.less$/i,
                    use: [
                        {
                            loader: MiniCssExtractPlugin.loader
                        },
                        {
                            loader: "css-loader",
                            options: {
                                url: true
                            }
                        },
                        {
                            loader: "less-loader",
                            options: {
                                implementation: require("less"),
                                sourceMap: false
                            }
                        }
                    ]
                },
                {
                    test: /\.scss$/,
                    use: [
                        {
                            loader: MiniCssExtractPlugin.loader
                        },
                        {
                            loader: "css-loader",
                            options: {
                                url: true
                            }
                        },
                        {
                            loader: 'resolve-url-loader',
                            options: {
                            }
                        },
                        {
                            loader: "sass-loader",
                            options: {
                                sourceMap: true, 
                            }
                        }
                    ]
                },
                { 
                    test: /\.css$/,
                    use: [
                        {
                            loader: isProductionMode
                                ? MiniCssExtractPlugin.loader
                                : 'style-loader'
                        },
                        {
                            loader: "css-loader",
                            options: {
                                url: true
                            }
                        }
                    ]
                },
                {
                    test: /\.tsx?$/,
                    use: {
                        loader: 'ts-loader',
                        options: {
                            transpileOnly: true,
                        },
                    },
                },
                {
                    test: /\.html$/,
                    use: {
                        loader: 'html-loader',
                        options: {
                            minimize: {
                                removeComments: false
                            }
                        }
                    }
                },
                {
                    test: /\.(ttf|eot|woff(2)?)(\?[a-z0-9]+)?$/,
                    type: "asset",
                    generator: {
                        filename: 'assets/fonts/[name].[hash:8][ext]',
                    }
                },
                {
                    test: /\.(png|jpg|jpeg|gif|svg)$/,
                    type: "asset/resource",
                    generator: {
                        filename: 'assets/img/[name].[hash:8][ext]'
                    }
                }
            ]
        },
        resolve: {
            modules: [path.resolve(__dirname, "../node_modules"), "node_modules"],
            extensions: ['.js', '.ts', '.tsx'],
            fallback: {
                fs: false
            },
            alias: {
                common: path.resolve(__dirname, 'typescript/common'),
                external: path.resolve(__dirname, 'typescript/external'),
                models: path.resolve(__dirname, 'typescript/models'),
                
                commands: path.resolve(__dirname, 'typescript/commands'),
                durandalPlugins: path.resolve(__dirname, 'typescript/durandalPlugins'),
                viewmodels: path.resolve(__dirname, 'typescript/viewmodels'),
                components: path.resolve(__dirname, 'typescript/components'),
                overrides: path.resolve(__dirname, 'typescript/overrides'),
                widgets: path.resolve(__dirname, 'typescript/widgets'),
                views: path.resolve(__dirname, 'wwwroot/App/views'),
                test: path.resolve(__dirname, 'typescript/test'),

                endpoints: path.resolve(__dirname, 'typings/server/endpoints'),
                configuration: path.resolve(__dirname, 'typings/server/configuration'),
                
                Content: path.resolve(__dirname, 'wwwroot/Content/'),
                wwwroot: path.resolve(__dirname, 'wwwroot/'),
                d3: path.resolve(__dirname, 'wwwroot/Content/custom_d3'),
                qrcodejs: path.resolve(__dirname, 'wwwroot/Content/custom_qrcode'),
                ["google.analytics"]: path.resolve(__dirname, 'wwwroot/Content/custom_ga'),
                
                Favico: path.resolve(__dirname, 'node_modules/favico.js/favico'),
                durandal: path.resolve(__dirname, 'node_modules/durandal/js'),
                jquery: path.resolve(__dirname, 'node_modules/jquery/dist/jquery'),
                plugins: path.resolve(__dirname, 'node_modules/durandal/js/plugins'),
                jwerty: path.resolve(__dirname, 'node_modules/jwerty-globals-fixed/jwerty'),


                hooks: path.resolve(__dirname, 'typescript/components/hooks'),
            }
        }
    };
};
