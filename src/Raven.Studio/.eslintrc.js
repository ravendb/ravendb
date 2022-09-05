module.exports = {
    "env": {
        "browser": true,
        "commonjs": true,
        "es2021": true
    },
    "extends": [
        "eslint:recommended",
        "plugin:react/recommended",
        "plugin:react-hooks/recommended",
        "plugin:@typescript-eslint/recommended",
        "prettier"
    ],
    "parser": "@typescript-eslint/parser",
    "parserOptions": {
        "ecmaFeatures": {
            "jsx": true
        },
        "ecmaVersion": "latest"
    },
    "plugins": [
        "react",
        "jest",
        "@typescript-eslint"
    ],
    "ignorePatterns": [
        "typescript/transitions/**/*.ts",
        "typescript/widgets/**/*.ts"
    ],
    "rules": {
        "react/prop-types": "off",
        "react/jsx-no-target-blank": "off",
        "@typescript-eslint/no-var-requires": "off",
        "@typescript-eslint/triple-slash-reference": "off",
        "@typescript-eslint/no-explicit-any": "off",
        "react/jsx-key": "off"
    },
    "settings": {
        "react": {
            "pragma": "React",
            "fragment": "Fragment",
            "version": "detect"
        }
    }
}
