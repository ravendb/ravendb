const path = require('path');
const webpack  = require("webpack");

module.exports = (env, argv) => {

    const isProductionMode = argv && argv.mode === 'production';
    
    return {
        mode: isProductionMode ? "production" : "development",
        entry: path.resolve(__dirname, './src/index.ts'),
        devtool: "source-map",
        module: {
            rules: [
                {
                    test: /\.tsx?$/,
                    use: {
                        loader: 'ts-loader',
                        options: {
                            configFile: path.resolve(__dirname, 'tsconfig.json')
                        }
                    },
                    exclude: /node_modules/,
                },
            ],
        },
        plugins: [
            new webpack.DefinePlugin({
                'process.env.NODE_ENV': JSON.stringify('development'),
                'process.env.NODE_DEBUG': false
            })
        ],
        output: {
            filename: 'rql_worker.js',
            path: path.resolve(__dirname, '../wwwroot'),
        },
        resolve: {
            extensions: ['.tsx', '.ts', '.js'],
            fallback: {
                fs: false
            }
        }
    }
};
