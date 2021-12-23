module.exports = {
    'roots': [
        '<rootDir>'
    ],
    'moduleFileExtensions': [
        'ts',
        'tsx',
        'js',
        'jsx',
        'json'
    ],
    "testRegex": [ "(/__tests__/.*|(\\.|/)(spec))\\.[jt]sx?$" ],
    'transform': {
        '.*\.tsx?$': 'ts-jest'
    },
    "setupFilesAfterEnv": [
      "jest-extended"
    ],
    moduleDirectories: [
        "node_modules",
        "<rootDir>/typescript"
    ],
    moduleNameMapper: {
        "^common/(.*)$": "<rootDir>/typescript/common/$1",
        "^models/(.*)$": "<rootDir>/typescript/models/$1",
        "^d3$": "<rootDir>/wwwroot/Content/custom_d3"
    }
}
