const fixableMeta = {
    type: "problem",
    fixable: "code",
    schema: [],
};

const rules = {
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
};

module.exports = {
    meta: {
        name: "local-rules",
    },
    rules,
};
