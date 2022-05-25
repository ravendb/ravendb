module.exports = {
    'roots': [
        '<rootDir>'
    ],
    'testEnvironment': 'jsdom',
    'moduleFileExtensions': [
        'ts',
        'tsx',
        'js',
        'jsx',
        'json',
    ],
    "testRegex": [ "(/__tests__/.*|(\\.|/)(spec))\\.[jt]sx?$" ],
    'transform': {
        '.*\.tsx?$': 'ts-jest',
        "^.+\\.html?$": "html-loader-jest"
    },
    "setupFiles": [
        "./scripts/setup_jest.js",
    ],
    "setupFilesAfterEnv": [
        "./scripts/setup_runtime.ts",
        "jest-extended"
    ],
    moduleDirectories: [
        "node_modules",
        "<rootDir>/typescript"
    ],
    moduleNameMapper: {
        "^common/(.*)$": "<rootDir>/typescript/common/$1",
        "^views/(.*)$": "<rootDir>/wwwroot/App/views/$1",
        "^external/(.*)$": "<rootDir>/typescript/external/$1",
        "^models/(.*)$": "<rootDir>/typescript/models/$1",
        "^plugins/(.*)$": "<rootDir>/node_modules/durandal/js/plugins/$1",
        "^durandal/(.*)$": "<rootDir>/node_modules/durandal/js/$1",
        "^endpoints$":  "<rootDir>/typings/server/endpoints",
        "^d3$": "<rootDir>/wwwroot/Content/custom_d3",
        "\\.(css|less|scss)$": "<rootDir>/typescript/test/__mocks__/styleMock.js",
    },
    "reporters": [
        "default",
        "jest-junit"
    ]
}
