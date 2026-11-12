// Keeps typography going through <Heading>/<Text> (src/components/typography.tsx) instead of hand-written
// classes, so the scale stays the single source of truth. Scoped to plain <p>/<span>/<div>, the elements
// <Text> replaces, so shadcn primitives and icon color classes are untouched.
//
// .oxlintrc.json turns it off where raw classes are legitimate: the primitive layer (shadcn/ui), the
// typography components themselves, the embeddable widget package (it ships its own rq-* tokens and does
// not depend on @/components/typography), and Storybook infra chrome.
const TEXT_ELEMENTS = new Set(["p", "span", "div"]);
const RAW_TEXT_CLASSES =
    /\btext-(sm|xs)\b.*\btext-muted-foreground\b|\btext-muted-foreground\b.*\btext-(sm|xs)\b|\btext-sm\b.*\bfont-medium\b/s;
const HEADING_ELEMENT = /^h[1-6]$/;

const useTypographyComponents = {
    meta: {
        type: "suggestion",
        schema: [],
        messages: {
            rawTextClasses:
                "Use <Text> (variant muted/caption/label) from @/components/typography instead of raw text-sm/text-xs/text-muted-foreground/font-medium classes.",
            headingClasses:
                "Style headings with <Heading variant=…> from @/components/typography, not classes on a raw <hN>.",
        },
    },
    create(context) {
        return {
            JSXAttribute(node) {
                const element = node.parent;
                if (node.name.name !== "className" || element.name.type !== "JSXIdentifier") {
                    return;
                }

                const elementName = element.name.name;
                if (HEADING_ELEMENT.test(elementName)) {
                    context.report({ node, messageId: "headingClasses" });
                    return;
                }

                const isRawTextClassName =
                    TEXT_ELEMENTS.has(elementName) &&
                    node.value?.type === "Literal" &&
                    RAW_TEXT_CLASSES.test(node.value.value);
                if (isRawTextClassName) {
                    context.report({ node: node.value, messageId: "rawTextClasses" });
                }
            },
        };
    },
};

// msw-storybook-addon types `parameters.msw` loosely, so only mockHandlers() checks the service names. With a
// mistyped name, the story's handler lands after the defaults and is silently ignored.
const useMockHandlers = {
    meta: {
        type: "problem",
        schema: [],
        messages: {
            rawMswParameter:
                "Build the msw parameter with mockHandlers() from @/mocks/default-mocks, so a mistyped service name is a compile error.",
        },
    },
    create(context) {
        return {
            Property(node) {
                const isMswKey = !node.computed && node.key.type === "Identifier" && node.key.name === "msw";
                if (!isMswKey) {
                    return;
                }

                const isMockHandlersCall =
                    node.value.type === "CallExpression" &&
                    node.value.callee.type === "Identifier" &&
                    node.value.callee.name === "mockHandlers";
                if (!isMockHandlersCall) {
                    context.report({ node: node.value, messageId: "rawMswParameter" });
                }
            },
        };
    },
};

export default {
    meta: {
        name: "local-rules",
    },
    rules: {
        "use-typography-components": useTypographyComponents,
        "use-mock-handlers": useMockHandlers,
    },
};
