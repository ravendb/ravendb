const fixableMeta = {
    type: "problem",
    fixable: "code",
    schema: [],
};

function createDeprecatedReactstrapImport({ context, name, canFix = true }) {
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
                message: `${name} import from reactstrap is deprecated. Use 'import ${name} from "react-bootstrap/${name}"' instead.`,
                fix: canFix ? fix : undefined,
            });
        },
    };
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
                JSXOpeningElement: function(node) {
                    const nodeName = node.name?.name;
                    if (nodeName === "Button" || nodeName === "ButtonWithSpinner") {
                        const colorProp = node?.attributes.find(x => x?.name?.name === "color");
                        const outlineProp = node?.attributes.find(x => x?.name?.name === "outline");

                        if (colorProp?.value?.type === "Literal") {
                            const colorValue = colorProp.value?.value;
                            const replacement = outlineProp && colorValue
                              ? `variant="outline-${colorValue}"`
                              : "variant";

                            context.report({
                                node: node,
                                message: "'color' is deprecated since we are migrating to react-bootstrap. Use 'variant' prop.",
                                fix(fixer) {
                                    const fixes = [fixer.replaceText(colorProp, replacement)];
                                    if (outlineProp) {
                                        fixes.push(fixer.remove(outlineProp));
                                    }
                                    return fixes;
                                },
                            });
                        } else if (colorProp) {
                            context.report({
                                node,
                                message: `'color' is deprecated since we are migrating to react-bootstrap. Use 'variant' prop. Fix cannot be used because color value is not Literal.`,
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
        create: (context) => createDeprecatedReactstrapImport({ context, name: "UncontrolledTooltip", canFix: false }),
    },
};
