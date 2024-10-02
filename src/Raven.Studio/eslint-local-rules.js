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
};
