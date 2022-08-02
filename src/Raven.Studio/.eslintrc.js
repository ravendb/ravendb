module.exports = {
    "env": {
        "browser": true,
        "commonjs": true,
        "es2016": true
    },
    "extends": [
        "eslint:recommended",
        "plugin:@typescript-eslint/recommended"
    ],
    "parser": "@typescript-eslint/parser",
    "parserOptions": {
    },
    "plugins": [
        "@typescript-eslint"
    ],
    "rules": {
        "@typescript-eslint/no-var-requires": "off",
        "@typescript-eslint/triple-slash-reference": "off"
    }
}
