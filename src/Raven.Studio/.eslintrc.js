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
  "plugins": ["react", "jest", "@typescript-eslint", "local-rules"],
  "ignorePatterns": ["typescript/transitions/**/*.ts", "typescript/widgets/**/*.ts"],
  "rules": {
    "react/prop-types": "off",
    "react/jsx-no-target-blank": "off",
    "@typescript-eslint/no-var-requires": "off",
    "@typescript-eslint/triple-slash-reference": "off",
    "@typescript-eslint/no-explicit-any": "off",
    "react/jsx-key": "off",
    "@typescript-eslint/prefer-namespace-keyword": "off",
    "@typescript-eslint/no-unused-vars": [
      "warn",
      {
        "argsIgnorePattern": "^_",
        "varsIgnorePattern": "^_",
        "caughtErrorsIgnorePattern": "^_"
      }
    ],
    "react/react-in-jsx-scope": "off",
    "react-hooks/exhaustive-deps": "off", // for now turned off (we must better handle DB switching, etc.)
    "local-rules/mixed-imports": "error",
    "curly": "warn",
    "react/jsx-curly-brace-presence": [
      'warn',
      { props: 'never', children: 'never' },
    ],
    "no-restricted-imports": [
      "error",
      {
        "paths": [
          {
            "name": "react-bootstrap",
            "message": "Please import individual components, e.g.: import Tooltip from 'react-bootstrap/Tooltip'"
          }
        ]
      }
    ],
    "no-constant-condition": "off"
  },
  "settings": {
    "react": {
      "pragma": "React",
      "fragment": "Fragment",
      "version": "detect"
    }
  }
};
