"use strict";
module.exports = ({ template }) => {
    const moduleExportsDeclaration = template(`
    export default ASSIGNMENT;
  `);
    const importDeclaration = template(`import SOURCE from "TARGET"; `);
    return {
        name: 'replace-ts-export-assignment',
        visitor: {
            TSExportAssignment(path) {
                path.replaceWith(moduleExportsDeclaration({ ASSIGNMENT: path.node.expression }));
            },
            TSImportEqualsDeclaration(path) {
                const rhs = path.get("moduleReference").getSource();
                if (rhs.startsWith("Raven.")) {
                    path.remove();
                    return;
                }
                if (path.node.moduleReference.type === "TSExternalModuleReference") {
                    const newValue = importDeclaration({ SOURCE: path.node.id.name, TARGET: path.node.moduleReference.expression.value });
                    path.replaceWith(newValue);
                }
            }
        }
    };
};
