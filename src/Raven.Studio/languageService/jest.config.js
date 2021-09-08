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
    'transform': {
        '.*\.tsx?$': 'ts-jest'
    },
    "setupFilesAfterEnv": [
      "jest-extended"
    ]
}
