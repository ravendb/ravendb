const fixableMeta = {
    type: "problem",
    fixable: "code",
    schema: [],
};

function createDeprecatedReactstrapImport({ context, name, reactBootstrapName = name, canFix = true }) {
    return {
        ImportDeclaration(node) {
            if (node.source.value !== "reactstrap") {
                return;
            };

            const specifiers = node.specifiers.filter(
                (specifier) =>
                    specifier.type === "ImportSpecifier" &&
                    specifier.imported.name === name
            );

            if (specifiers.length === 0) {
                return;
            }

            const fix = (fixer) => {
                const fixes = [];
                const sourceCode = context.getSourceCode();

                if (node.specifiers.length === specifiers.length) {
                    fixes.push(
                        fixer.replaceText(
                            node,
                            `import ${name} from "react-bootstrap/${name}";`
                        )
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
                            `import ${name} from "react-bootstrap/${name}";\n`
                        )
                    );
                }
                return fixes;
            };

            context.report({
                node: node,
                message: `${name} import from reactstrap is deprecated. Use 'import ${reactBootstrapName} from "react-bootstrap/${reactBootstrapName}"' instead.`,
                fix: canFix ? fix : undefined,
            });
        },
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

module.exports = {
    "no-reactstrap-alert": {
        create: function (context) {
            return {
                JSXIdentifier: function (node) {
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
        create: function (context) {
            return {
                TSExportAssignment: function (node) {
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
                                }
                            }
                            
                            context.report({
                                node: invalidImport,
                                message: "Imports/from mixed with export=, use 'import a = require(...)' or avoid 'export = X'",
                                fix
                            });
                        })
                        
                       
                    }
                },
            }
        }
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
                                        ` variant="secondary"`
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
                                        `variant="${colorValue}"`
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
        create: (context) => createDeprecatedReactstrapImport({ context, name: "UncontrolledTooltip", reactBootstrapName: "OverlayTrigger", canFix: false }),
    },
    "no-reactstrap-UncontrolledPopover": {
        create: (context) => createDeprecatedReactstrapImport({ context, name: "UncontrolledPopover", reactBootstrapName: "OverlayTrigger", canFix: false }),
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
                    if (node.name?.name !== "Badge") {return;}

                    let hasBg = false;
                    node.attributes.forEach(attr => {
                        if (!attr || !attr.name || !attr.name.name) {return;}
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
        create: (context) => createDeprecatedReactstrapImport({ context, name: "UncontrolledAccordion", reactBootstrapName: "Accordion", canFix: false }),
    },
};
