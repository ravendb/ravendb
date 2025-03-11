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
    "@typescript-eslint/no-unused-vars": "warn",
    "react/react-in-jsx-scope": "off",
    "local-rules/no-reactstrap-alert": "warn",
    "local-rules/mixed-imports": "warn",
    "local-rules/no-reactstrap-Button-color-prop": "warn",
    "local-rules/no-reactstrap-Button": "warn",
    "local-rules/no-reactstrap-Spinner": "warn",
    "local-rules/no-reactstrap-UncontrolledTooltip": "warn",
    "local-rules/no-reactstrap-UncontrolledPopover": "warn",
    "local-rules/no-reactstrap-Tooltip": "warn",
    "local-rules/no-reactstrap-Popover": "warn",
    "local-rules/no-reactstrap-Badge": "warn",
    "local-rules/no-reactstrap-Badge-props": "warn",
    "local-rules/no-reactstrap-Collapse": "warn",
    "local-rules/no-reactstrap-Collapse-props": "warn",
    "local-rules/no-reactstrap-ButtonGroup": "warn",
    "local-rules/no-reactstrap-Pagination": "warn",
    "local-rules/no-reactstrap-Alert": "warn",
    "local-rules/no-reactstrap-ListGroup": "warn",
    "local-rules/no-reactstrap-Accordion": "warn",
    "local-rules/no-reactstrap-Table": "warn",
    "local-rules/no-reactstrap-Card": "warn",
    "local-rules/no-reactstrap-CardHeader": "warn",
    "local-rules/no-reactstrap-CardBody": "warn",
    "local-rules/reactstrap-Card-children-to-RBootstrap-children": "warn",
    "local-rules/no-reactstrap-Nav": "warn",
    "local-rules/no-reactstrap-InputGroup": "warn",
    "local-rules/no-reactstrap-InputGroupText": "warn",
    "local-rules/no-reactstrap-Carousel": "warn",
    "local-rules/no-reactstrap-Form": "warn",
    "local-rules/no-reactstrap-FormGroup": "warn",
    "local-rules/no-reactstrap-Input": "warn",
    "local-rules/reactstrap-Form-to-RBootstrap-children": "warn",
    "local-rules/react-bootstrap-FormControl-checkbox-to-FormCheck": "warn",
    "local-rules/no-reactstrap-Row": "warn",
    "local-rules/no-reactstrap-Col": "warn",
    "local-rules/no-reactstrap-Dropdown": "warn",
    "local-rules/no-reactstrap-UncontrolledDropdown": "warn",
    "local-rules/no-reactstrap-DropdownToggle": "warn",
    "local-rules/no-reactstrap-DropdownMenu": "warn",
    "local-rules/no-reactstrap-DropdownItem": "warn",
    "local-rules/no-reactstrap-Dropdown-childrens": "warn",
    "local-rules/no-reactstrap-DropdownToggle-color-prop": "warn",
    "local-rules/no-reactstrap-DropdownToggle-caret-prop": "warn",
    "local-rules/no-reactstrap-Modal": "warn",
    "local-rules/no-reactstrap-Label": "warn",
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
    ]
  },
  "settings": {
    "react": {
      "pragma": "React",
      "fragment": "Fragment",
      "version": "detect"
    }
  }
};
