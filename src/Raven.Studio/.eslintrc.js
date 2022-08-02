module.exports = {
    "env": {
        "browser": true,
        "commonjs": true,
        "es2021": true
    },
    "extends": [
        "eslint:recommended",
        "plugin:@typescript-eslint/recommended"
    ],
    "parser": "@typescript-eslint/parser",
    "parserOptions": {
        "ecmaVersion": "latest"
    },
    "plugins": [
        "@typescript-eslint"
    ],
    "ignorePatterns": [
        "typescript/transitions/**/*.ts"
    ],
    "rules": {
        "@typescript-eslint/no-var-requires": "off",
        "@typescript-eslint/triple-slash-reference": "off"
    }
}
