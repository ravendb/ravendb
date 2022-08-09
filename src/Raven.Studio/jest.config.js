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
        "jest-extended/all",
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
        "^hooks/(.*)$": "<rootDir>/typescript/components/hooks/$1",
        "\\.(css|less|scss)$": "<rootDir>/typescript/test/__mocks__/styleMock.js",
        "\\.(jpg|jpeg|png|gif|eot|otf|webp|svg|ttf|woff|woff2|mp4|webm|wav|mp3|m4a|aac|oga|docx|pdf)$": "<rootDir>/typescript/test/__mocks__/fileMock.js",
    },
    "reporters": [
        "default",
        "jest-junit"
    ]
}
