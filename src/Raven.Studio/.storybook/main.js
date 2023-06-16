const webpackConfigFunc = require('../webpack.config');
const path = require("path");
const CopyPlugin = require("copy-webpack-plugin");

const webpackConfig = webpackConfigFunc(null, {
    mode: "development",
    watch: false
});

const customHooksToMock = require("../typescript/components/hooks/hooksForAutoMock.json").hooks;
const customHooksAliases = {};

customHooksToMock.forEach(name => {
   customHooksAliases["hooks/" + name] = path.resolve(__dirname, "../typescript/components/hooks/__mocks__/" + name);
});

module.exports = {
  core: {
    builder: "webpack5"
  },
    babel: async (options) => {
      options.plugins.push(require.resolve("./import_plugin.js"));
      return {
          ...options,
          sourceType: "unambiguous"
      }
    },
  "stories": [
    "../typescript/**/*.stories.tsx"
  ],
  "addons": [
    "@storybook/addon-links",
    "@storybook/addon-essentials",
    "@storybook/addon-interactions"
  ],
  "framework": "@storybook/react",
    webpackFinal: async config => {
        config.resolve.alias = {
            ...customHooksAliases,
            ...config.resolve.alias, 
            ...webpackConfig.resolve.alias,
        };

        config.output.publicPath = "/";
        
        config.plugins.unshift(webpackConfig.plugins.find(x => x.constructor.name === "ProvidePlugin"));

        const incomingRules = webpackConfig.module.rules
            .filter(x => (x.use && x.use.indexOf && x.use.indexOf("imports-loader") === 0)
                || (x.use && x.use.loader === "html-loader")
                || (x.type && x.type === "asset/source")
                || (x.test && x.test.toString().includes(".less"))
                || (x.test && x.test.toString().includes(".font\\.js"))
                || (x.test && x.test.toString().includes(".scss")));
        
        config.plugins.push(webpackConfig.plugins[0]); // MiniCssExtractPlugin

        const copyPlugin = new CopyPlugin({
          patterns: [
                {
                  from: path.resolve(__dirname, '../wwwroot/Content/ace/'),
                  to: './ace/'
                },
            ]
        });

        config.plugins.push(copyPlugin);
        
        config.module.rules.push(...incomingRules);
        
        return config;
    }
}
