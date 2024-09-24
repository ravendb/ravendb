module.exports = {
  "env": {
    "browser": true,
    "commonjs": true,
    "es2021": true,
    "node": true,
  },
  "extends": ["eslint:recommended", "plugin:react/recommended", "plugin:react-hooks/recommended", "plugin:@typescript-eslint/recommended", "prettier", "plugin:storybook/recommended"],
  "parser": "@typescript-eslint/parser",
  "parserOptions": {
    "ecmaFeatures": {
      "jsx": true
    },
    "ecmaVersion": "latest"
  },
  "plugins": ["react", "jest", "@typescript-eslint"],
  "ignorePatterns": ["typescript/transitions/**/*.ts", "typescript/widgets/**/*.ts"],
  "rules": {
    "react/prop-types": "off",
    "react/jsx-no-target-blank": "off",
    "@typescript-eslint/no-var-requires": "off",
    "@typescript-eslint/triple-slash-reference": "off",
    "@typescript-eslint/no-explicit-any": "off",
    "react/jsx-key": "off",
    "@typescript-eslint/prefer-namespace-keyword": "off",
    "@typescript-eslint/no-unused-vars": "warn",
    "react/react-in-jsx-scope": "off"
  },
  "settings": {
    "react": {
      "pragma": "React",
      "fragment": "Fragment",
      "version": "detect"
    }
  }
};
