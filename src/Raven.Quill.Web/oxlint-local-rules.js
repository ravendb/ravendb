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

export default {
    meta: {
        name: "local-rules",
    },
    rules: {
        "use-typography-components": useTypographyComponents,
    },
};
