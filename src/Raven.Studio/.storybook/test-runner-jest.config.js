const { getJestConfig } = require("@storybook/test-runner");

const testRunnerConfig = getJestConfig();
const jestPlaywrightConfig = testRunnerConfig.testEnvironmentOptions["jest-playwright"];

module.exports = {
    ...testRunnerConfig,
    testEnvironmentOptions: {
        ...testRunnerConfig.testEnvironmentOptions,
        "jest-playwright": {
            ...jestPlaywrightConfig,
            // GitHub's Ubuntu runner already includes Chrome, so CI does not need a Playwright browser download.
            launchOptions: process.env.GITHUB_ACTIONS === "true" ? { channel: "chrome" } : {},
        },
    },
};
