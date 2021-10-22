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

const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;

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
            "window.ravenStudioRelease": isProductionMode
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
        devtool: isProductionMode ? "source-map" : "eval",
        entry: {
            main: "./typescript/main.ts",
            "styles": "./wwwroot/Content/css/styles.less",
            "styles-blue": "./wwwroot/Content/css/styles-blue.less",
            "styles-light": "./wwwroot/Content/css/styles-light.less"
        },
        output: {
            path: __dirname + '/wwwroot/dist',
            filename: isProductionMode ? 'assets/[name].[contenthash:8].js' : 'assets/[name].js',
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
                    test: /\.ts$/,
                    use: 'ts-loader'
                },
                {
                    test: /\.html$/,
                    use: {
                        loader: 'raw-loader',
                        options: {
                            esModule: false,
                        },
                    }
                },
                {
                    test: /\.(ttf|eot|svg|woff(2)?)(\?[a-z0-9]+)?$/,
                    loader: 'file-loader',
                    options: {
                        name: "[name].[hash:8].[ext]",
                        outputPath: "assets/fonts/"
                    }
                },
                {
                    test: /\.(png|jpg|jpeg|gif|svg)$/,
                    use: [{
                        loader: 'url-loader',
                        options: {
                            name: 'assets/img/[name].[hash:8].[ext]',
                            limit: 8192,
                            esModule: false
                        }
                    }]
                }
            ]
        },
        resolve: {
            modules: [path.resolve(__dirname, "../node_modules"), "node_modules"],
            extensions: ['.js', '.ts', '.tsx'],
            alias: {
                common: path.resolve(__dirname, 'typescript/common'),
                models: path.resolve(__dirname, 'typescript/models'),
                commands: path.resolve(__dirname, 'typescript/commands'),
                durandalPlugins: path.resolve(__dirname, 'typescript/durandalPlugins'),
                viewmodels: path.resolve(__dirname, 'typescript/viewmodels'),
                overrides: path.resolve(__dirname, 'typescript/overrides'),
                widgets: path.resolve(__dirname, 'typescript/widgets'),
                views: path.resolve(__dirname, 'wwwroot/App/views'),

                endpoints: path.resolve(__dirname, 'typings/server/endpoints'),
                configuration: path.resolve(__dirname, 'typings/server/configuration'),
                
                Content: path.resolve(__dirname, 'wwwroot/Content/'),
                d3: path.resolve(__dirname, 'wwwroot/Content/custom_d3'),
                qrcodejs: path.resolve(__dirname, 'wwwroot/Content/custom_qrcode'),
                ["google.analytics"]: path.resolve(__dirname, 'wwwroot/Content/custom_ga'),
                
                Favico: path.resolve(__dirname, 'node_modules/favico.js/favico'),
                durandal: path.resolve(__dirname, 'node_modules/durandal/js'),
                plugins: path.resolve(__dirname, 'node_modules/durandal/js/plugins'),
                jwerty: path.resolve(__dirname, 'node_modules/jwerty-globals-fixed/jwerty')
            }
        }
    };
};
