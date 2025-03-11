const fixableMeta = {
  type: "problem",
  fixable: "code",
  schema: [],
};

function createDeprecatedReactstrapImport({ context, name, reactBootstrapName = name, canFix = true, newImport }) {
  return {
    ImportDeclaration(node) {
      if (node.source.value !== "reactstrap") {
        return;
      }

      const specifiers = node.specifiers.filter(
        (specifier) =>
          specifier.type === "ImportSpecifier" &&
          specifier.imported.name === name,
      );

      if (specifiers.length === 0) {
        return;
      }

      const sourceCode = context.getSourceCode();
      const existingImport = sourceCode.ast.body.find(
        (n) =>
          n.type === "ImportDeclaration" &&
          n.source.value === `react-bootstrap/${reactBootstrapName}`,
      );

      const fix = (fixer) => {
        const fixes = [];

        if (existingImport) {
          specifiers.forEach((specifier) => {
            let [start, end] = specifier.range;

            const tokenBefore = sourceCode.getTokenBefore(specifier);
            if (tokenBefore && tokenBefore.value === ",") {
              start = tokenBefore.range[0];
            } else {
              const tokenAfter = sourceCode.getTokenAfter(specifier);
              if (tokenAfter && tokenAfter.value === ",") {
                end = tokenAfter.range[1];
              }
            }
            fixes.push(fixer.removeRange([start, end]));
          });
        } else {
          if (node.specifiers.length === specifiers.length) {
            fixes.push(
              fixer.replaceText(
                node,
                newImport || `import ${reactBootstrapName} from "react-bootstrap/${reactBootstrapName}";`,
              ),
            );
          } else {
            specifiers.forEach((specifier) => {
              let [start, end] = specifier.range;

              const tokenBefore = sourceCode.getTokenBefore(specifier);
              if (tokenBefore && tokenBefore.value === ",") {
                start = tokenBefore.range[0];
              } else {
                const tokenAfter = sourceCode.getTokenAfter(specifier);
                if (tokenAfter && tokenAfter.value === ",") {
                  end = tokenAfter.range[1];
                }
              }
              fixes.push(fixer.removeRange([start, end]));
            });

            fixes.push(
              fixer.insertTextBefore(
                node,
                newImport || `import ${reactBootstrapName} from "react-bootstrap/${reactBootstrapName}";\n`,
              ),
            );
          }
        }
        return fixes;
      };

      context.report({
        node: node,
        message: `${name} import from reactstrap is deprecated. Use 'import ${reactBootstrapName} from "react-bootstrap/${reactBootstrapName}"' instead.`,
        fix: canFix ? fix : undefined,
      });
    },

    JSXIdentifier(node) {
      if (name !== reactBootstrapName && node.name === name) {
        context.report({
          node: node,
          message: `Replace '${name}' with '${reactBootstrapName}'.`,
          fix: canFix ? (fixer) => fixer.replaceText(node, reactBootstrapName) : undefined,
        });
      }
    }
  };
}
function migrateProp({ attr, newProp, message, context }) {
  context.report({
    node: attr,
    message,
    fix(fixer) {
      return fixer.replaceText(attr.name, newProp);
    },
  });
}

function removeProp({ attr, message, context }) {
  context.report({
    node: attr,
    message,
    fix(fixer) {
      return fixer.remove(attr);
    },
  });
}

function replaceReactstrapToReactBootstrap({ context, componentMap }) {
  return {
    JSXIdentifier(node) {
      if (componentMap[node.name]) {
        context.report({
          node,
          message: `Replace '${node.name}' with '${componentMap[node.name]}'.`,
          fix: (fixer) => {
            const replacement = componentMap[node.name];

            if (replacement) {
              return fixer.replaceText(node, replacement);
            }
          },
        });
      }
    },
  };
}

/**
 * Handles the properties of a JSX component and applies transformations based on the provided configuration.
 *
 * @param {Object} config - The configuration object containing properties to remove or migrate.
 * @param {string[]} [config.toRemove] - Array of property names to be removed from the component.
 * @param {Array<{key: string, migrateTo: string}>} [config.toMigrate] - Array of property migration objects,
 *                                                                              each containing the original property name (key)
 *                                                                              and the new property name (migrateTo).
 * @param {string} componentName - The name of the component to handle properties for.
 *                                        Can be a simple component name (e.g., "Button") or a
 *                                        namespaced component (e.g., "Form.Control").
 */
function handleProps({ context, config, componentName }) {
  return {
    JSXOpeningElement(node) {
      if (componentName.includes(".")) {
        const [parentName, childName] = componentName.split(".");
        if (
          !(
            node.name &&
            node.name.type === "JSXMemberExpression" &&
            node.name.object &&
            node.name.object.name === parentName &&
            node.name.property &&
            node.name.property.name === childName
          )
        ) {
          return;
        }
      } else {
        if (!(node.name && node.name.type === "JSXIdentifier" && node.name.name === componentName)) {
          return;
        }
      }

      node.attributes.forEach((attr) => {
        if (!attr || !attr.name || !attr.name.name) {
          return;
        }
        const propName = attr.name.name;

        if (config.toRemove && config.toRemove.includes(propName)) {
          context.report({
            node: attr,
            message: `'${propName}' prop is not supported and should be removed.`,
            fix(fixer) {
              return fixer.remove(attr);
            },
          });
        }

        if (config.toMigrate) {
          const migration = config.toMigrate.find((m) => m.key === propName);
          if (migration) {
            context.report({
              node: attr,
              message: `'${propName}' prop is deprecated. Use '${migration.migrateTo}' instead.`,
              fix(fixer) {
                return fixer.replaceText(attr.name, migration.migrateTo);
              },
            });
          }
        }
      });
    },
  };
}

module.exports = {
  "no-reactstrap-alert": {
    create: function(context) {
      return {
        JSXIdentifier: function(node) {
          if (node.name === "Alert") {
            context.report({
              node: node,
              message: "Use 'RichAlert' instead of 'Alert'",
            });
          }
        },
      };
    },
  },
  "mixed-imports": {
    meta: fixableMeta,
    create: function(context) {
      return {
        TSExportAssignment: function(node) {
          const program = node.parent;

          const invalidImports = program.body.filter(x => x.type === "ImportDeclaration");
          if (invalidImports.length > 0) {
            invalidImports.forEach(invalidImport => {
              /**
               * @type {(fixer:RuleFixer) => void}
               */
              let fix = null;
              const specifiers = invalidImport.specifiers;
              if (specifiers.length === 1 && specifiers[0].type === "ImportDefaultSpecifier") {
                fix = (fixer) => {
                  const newText = "import " + specifiers[0].local.name + " = require(\"" + invalidImport.source.value + "\");";
                  return fixer.replaceText(invalidImport, newText);
                };
              }

              context.report({
                node: invalidImport,
                message: "Imports/from mixed with export=, use 'import a = require(...)' or avoid 'export = X'",
                fix,
              });
            });


          }
        },
      };
    },
  },
  "no-reactstrap-Button-color-prop": {
    meta: fixableMeta,
    create(context) {
      return {
        JSXOpeningElement(node) {
          const nodeName = node.name?.name;

          if (nodeName === "Button" || nodeName === "ButtonWithSpinner") {
            const hasVariantProp = node.attributes.some(attr => attr?.name?.name === "variant");
            const colorProp = node.attributes.find(attr => attr?.name?.name === "color");

            if (!hasVariantProp && !colorProp) {
              context.report({
                node,
                message: "Button is missing 'variant' prop. Defaulting to 'variant=\"secondary\"'.",
                fix(fixer) {
                  return fixer.insertTextAfter(
                    node.name,
                    ` variant="secondary"`,
                  );
                },
              });
            } else if (colorProp?.value?.type === "Literal") {
              const colorValue = colorProp.value.value;
              context.report({
                node: colorProp,
                message: `'color' is deprecated. Replace with 'variant="${colorValue}"'.`,
                fix(fixer) {
                  return fixer.replaceText(
                    colorProp,
                    `variant="${colorValue}"`,
                  );
                },
              });
            } else if (colorProp) {
              context.report({
                node: colorProp,
                message: `'color' is deprecated, but automatic fix is not possible as value is not Literal.`,
              });
            }
          }
        },
      };
    },
  },
  "no-reactstrap-Button": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Button" }),
  },
  "no-reactstrap-Spinner": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Spinner" }),
  },
  "no-reactstrap-UncontrolledTooltip": {
    create: (context) => createDeprecatedReactstrapImport({
      context,
      name: "UncontrolledTooltip",
      reactBootstrapName: "OverlayTrigger",
      canFix: false,
    }),
  },
  "no-reactstrap-UncontrolledPopover": {
    create: (context) => createDeprecatedReactstrapImport({
      context,
      name: "UncontrolledPopover",
      reactBootstrapName: "OverlayTrigger",
      canFix: false,
    }),
  },
  "no-reactstrap-Tooltip": {
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Tooltip", canFix: false }),
  },
  "no-reactstrap-Popover": {
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Popover", canFix: false }),
  },
  "no-reactstrap-Badge": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Badge" }),
  },
  "no-reactstrap-Badge-props": {
    meta: fixableMeta,
    create(context) {
      const sourceCode = context.getSourceCode();

      return {
        JSXOpeningElement(node) {
          if (node.name?.name !== "Badge") {
            return;
          }

          let hasBg = false;
          node.attributes.forEach(attr => {
            if (!attr || !attr.name || !attr.name.name) {
              return;
            }
            const propName = attr.name.name;

            if (propName === "bg" || propName === "color") {
              hasBg = true;
            }

            switch (propName) {
              case "color":
                migrateProp({
                  attr,
                  newProp: "bg",
                  message: "'color' prop is deprecated. Use 'bg' prop instead.",
                  context,
                });
                break;
              case "type":
                migrateProp({
                  attr,
                  newProp: "as",
                  message: "'type' prop is deprecated. Use 'as' prop instead.",
                  context,
                });
                break;
              case "node":
              case "cssModule":
              case "innerRef":
                removeProp(
                  {
                    context,
                    attr,
                    message: `'${propName}' prop is not supported in react-bootstrap Badge.`,
                  },
                );
                break;
              default:
                break;
            }
          });

          if (!hasBg) {
            context.report({
              node,
              message: "'Badge' is missing 'bg' prop, defaulting to 'secondary'.",
              fix(fixer) {
                const lastToken = sourceCode.getLastToken(node);
                return fixer.insertTextBefore(lastToken, " bg=\"secondary\"");
              },
            });
          }
        },
      };
    },
  },
  "no-reactstrap-Collapse": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Collapse" }),
  },
  "no-reactstrap-Collapse-props": {
    meta: fixableMeta,
    create(context) {
      return {
        JSXOpeningElement(node) {
          if (node.name?.name !== "Collapse") {
            return;
          }

          node.attributes.forEach(attr => {
            if (!attr || !attr.name || !attr.name.name) {
              return;
            }

            const propName = attr.name.name;

            if (propName === "isOpen") {
              migrateProp({
                context,
                attr,
                message: "'isOpen' prop is deprecated. Use 'in' prop instead.",
                newProp: "in",
              });
            }
          });
        },
      };
    },
  },
  "no-reactstrap-ButtonGroup": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "ButtonGroup" }),
  },
  "no-reactstrap-Pagination": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Pagination" }),
  },
  "no-reactstrap-Alert": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Alert" }),
  },
  "no-reactstrap-ListGroup": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "ListGroup" }),
  },
  "no-reactstrap-Accordion": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({
      context,
      name: "UncontrolledAccordion",
      reactBootstrapName: "Accordion",
      canFix: false,
    }),
  },
  "no-reactstrap-Table": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Table" }),
  },
  "no-reactstrap-Card": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Card" }),
  },
  "no-reactstrap-CardHeader": {
    meta: fixableMeta,
    create: (create) => createDeprecatedReactstrapImport({
      context: create,
      name: "CardHeader",
      reactBootstrapName: "Card",
    }),
  },
  "no-reactstrap-CardBody": {
    meta: fixableMeta,
    create: (create) => createDeprecatedReactstrapImport({
      context: create,
      name: "CardBody",
      reactBootstrapName: "Card",
    }),
  },
  "reactstrap-Card-children-to-RBootstrap-children": {
    meta: fixableMeta,
    create(context) {
      const cardComponentMap = {
        CardBody: "Card.Body",
        CardHeader: "Card.Header",
        CardTitle: "Card.Title",
        CardText: "Card.Text",
        CardSubtitle: "Card.Subtitle",
        CardImg: "Card.Img",
        CardFooter: "Card.Footer",
      };

      return replaceReactstrapToReactBootstrap({ context, componentMap: cardComponentMap });
    },
  },
  "no-reactstrap-Nav": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Nav" }),
  },
  "reactstrap-Nav-children-to-RBootstrap-children": {
    meta: fixableMeta,
    create(context) {
      const navComponentMap = {
        NavItem: "Nav.Item",
      };

      return replaceReactstrapToReactBootstrap({ context, componentMap: navComponentMap });
    },
  },
  "no-reactstrap-InputGroup": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "InputGroup" }),
  },
  "no-reactstrap-InputGroupText": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "InputGroupText", canFix: false }),
  },
  "no-reactstrap-Carousel": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Carousel", canFix: false }),
  },
  "no-reactstrap-Form": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Form" }),
  },
  "reactstrap-Form-to-RBootstrap-children": {
    meta: fixableMeta,
    create: (context) => {
      const dropdownComponentMap = {
        FormGroup: "Form.Group",
        Input: "Form.Control",
      };

      return replaceReactstrapToReactBootstrap({ context, componentMap: dropdownComponentMap });
    },
  },
  "no-reactstrap-FormControl-props": {
    meta: fixableMeta,
    create: (context) => {
      const config = {
        toMigrate: [{
          key: "invalid", migrateTo: "isInvalid",
        }, {
          key: "valid", migrateTo: "isValid",
        }, {
          key: "bsSize", migrateTo: "size",
        }, {
          key: "innerRef", migrateTo: "ref",
        }],
      };

      return handleProps({ context, config, componentName: "Form.Control" });
    },
  },
  "no-reactstrap-FormGroup": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "FormGroup", reactBootstrapName: "Form" }),
  },
  "no-reactstrap-Input": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Input", reactBootstrapName: "Form" }),
  },
  "react-bootstrap-FormControl-checkbox-to-FormCheck": {
    meta: fixableMeta,
    create(context) {
      return {
        JSXOpeningElement(node) {
          if (
            node.name &&
            node.name.type === "JSXMemberExpression" &&
            node.name.object &&
            node.name.object.name === "Form" &&
            node.name.property &&
            node.name.property.name === "Control"
          ) {
            const typeAttr = node.attributes.find(
              attr => attr?.name?.name === "type" &&
                attr?.value?.type === "Literal" &&
                ["checkbox", "radio", "switch"].includes(attr.value.value),
            );

            if (typeAttr) {
              const inputType = typeAttr.value.value;

              context.report({
                node: node,
                message: `Form.Control with type="${inputType}" should be replaced with Form.Check`,
                fix(fixer) {
                  return fixer.replaceText(
                    node.name,
                    "Form.Check",
                  );
                },
              });
            }
          }
        },
      };
    },
  },
  "no-reactstrap-Row": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Row" }),
  },
  "no-reactstrap-Col": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Col" }),
  },
  "no-reactstrap-Dropdown": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Dropdown" }),
  },
  "no-reactstrap-UncontrolledDropdown": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({
      context,
      name: "UncontrolledDropdown",
      reactBootstrapName: "Dropdown",
      canFix: false,
    }),
  },
  "no-reactstrap-DropdownToggle": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({
      context,
      name: "DropdownToggle",
      reactBootstrapName: "Dropdown",
      canFix: false,
    }),
  },
  "no-reactstrap-DropdownMenu": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({
      context,
      name: "DropdownMenu",
      reactBootstrapName: "Dropdown",
      canFix: false,
    }),
  },
  "no-reactstrap-DropdownItem": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({
      context,
      name: "DropdownItem",
      reactBootstrapName: "Dropdown",
      canFix: false,
    }),
  },
  "no-reactstrap-Dropdown-childrens": {
    meta: fixableMeta,
    create: (context) => {
      const dropdownComponentMap = {
        DropdownItem: "Dropdown.Item",
        DropdownToggle: "Dropdown.Toggle",
        DropdownMenu: "Dropdown.Menu",
        UncontrolledDropdown: "Dropdown",
      };

      return replaceReactstrapToReactBootstrap({ context, componentMap: dropdownComponentMap });
    },
  },
  "no-reactstrap-DropdownToggle-color-prop": {
    meta: fixableMeta,
    create(context) {
      return {
        JSXOpeningElement(node) {
          // Check for Dropdown.Toggle component
          const isDropdownToggle = node.name?.type === "JSXMemberExpression" &&
            node.name.object?.name === "Dropdown" &&
            node.name.property?.name === "Toggle";

          if (isDropdownToggle) {
            const hasVariantProp = node.attributes.some(attr => attr?.name?.name === "variant");
            const colorProp = node.attributes.find(attr => attr?.name?.name === "color");

            if (!hasVariantProp && !colorProp) {
              context.report({
                node,
                message: "Component is missing 'variant' prop. Defaulting to 'variant=\"secondary\"'.",
                fix(fixer) {
                  return fixer.insertTextAfter(
                    node.name,
                    ` variant="secondary"`,
                  );
                },
              });
            } else if (colorProp?.value?.type === "Literal") {
              const colorValue = colorProp.value.value;
              context.report({
                node: colorProp,
                message: `'color' is deprecated. Replace with 'variant="${colorValue}"'.`,
                fix(fixer) {
                  return fixer.replaceText(
                    colorProp,
                    `variant="${colorValue}"`,
                  );
                },
              });
            } else if (colorProp) {
              context.report({
                node: colorProp,
                message: `'color' is deprecated, but automatic fix is not possible as value is not Literal.`,
              });
            }
          }
        },
      };
    },
  },
  "no-reactstrap-DropdownToggle-caret-prop": {
    meta: fixableMeta,
    create(context) {
      return {
        JSXOpeningElement(node) {
          const isDropdownToggle = node.name?.type === "JSXMemberExpression" &&
            node.name.object?.name === "Dropdown" &&
            node.name.property?.name === "Toggle";

          if (isDropdownToggle) {
            const caretProp = node.attributes.find(attr => attr?.name?.name === "caret");

            if (caretProp) {
              if (caretProp?.value?.type === "JSXExpressionContainer" &&
                caretProp.value.expression?.value === false) {
                context.report({
                  node: caretProp,
                  message: "'caret' prop is not supported in react-bootstrap. Use 'as' prop instead with a custom component.",
                  fix(fixer) {
                    return fixer.replaceText(
                      caretProp,
                      `as={CustomToggleComponent}`,
                    );
                  },
                });
              } else {
                context.report({
                  node: caretProp,
                  message: "'caret' prop is not needed in react-bootstrap as it shows caret by default.",
                  fix(fixer) {
                    return fixer.replaceText(caretProp, "isCaretHidden");
                  },
                });
              }
            }
          }
        },
      };
    },
  },
  "no-reactstrap-Modal": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Modal" }),
  },
  "no-reactstrap-Modal-props": {
    meta: fixableMeta,
    create: (context) => {
      const config = {
        toMigrate: [{
          key: "toggle", migrateTo: "onHide",
        }, {
          key: "className", migrateTo: "dialogClassName",
        }, {
          key: "cssModule", migrateTo: "dialogCssClass",
        }, {
          key: "isOpen", migrateTo: "show",
        }],
      };

      return handleProps({ context, config, componentName: "Modal" });
    }
  },
  "no-reactstrap-Label": {
    meta: fixableMeta,
    create: (context) => createDeprecatedReactstrapImport({ context, name: "Label", newImport: "import Label from \"components/common/Label\"" }),
  },
  "no-reactstrap-Label-props": {
    meta: fixableMeta,
    create: (context) => {
      const config = {
        toMigrate: [{
          key: "for", migrateTo: "htmlFor",
        }],
        toRemove: ["check"]
      };

      return handleProps({ context, config, componentName: "Label" });
    }
  }
};
