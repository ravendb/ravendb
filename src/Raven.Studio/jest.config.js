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
    ]
}
