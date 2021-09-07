const path = require('path');
const webpack  = require("webpack");

//TODO: add support for production mode (+ bind to build process)

module.exports = {
    mode: "development", 
    entry: path.resolve(__dirname, './src/index.ts'),
    devtool: 'inline-source-map',
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
    }
};
