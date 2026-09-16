import storybook from "eslint-plugin-storybook";
import js from "@eslint/js";
import globals from "globals";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import tseslint from "typescript-eslint";
import { defineConfig, globalIgnores } from "eslint/config";

export default defineConfig([
    globalIgnores(["dist", "storybook-static", ".storybook/public/mockServiceWorker.js"]),
    {
        files: ["**/*.{ts,tsx}"],
        extends: [
            js.configs.recommended,
            tseslint.configs.recommended,
            reactHooks.configs.flat.recommended,
            reactRefresh.configs.vite,
        ],
        languageOptions: {
            globals: globals.browser,
        },
        rules: {
            // Keep typography going through <Heading>/<Text> (src/components/typography.tsx) instead of
            // hand-written classes, so the scale stays the single source of truth. Scoped to plain
            // <p>/<span>/<div> — the elements <Text> replaces — so shadcn primitives and icon color
            // classes are untouched. The typography components themselves are exempt below.
            "no-restricted-syntax": [
                "error",
                {
                    selector:
                        "JSXOpeningElement:matches([name.name='p'], [name.name='span'], [name.name='div']) > JSXAttribute[name.name='className'] > Literal[value=/\\btext-(sm|xs)\\b[^\"]*\\btext-muted-foreground\\b|\\btext-muted-foreground\\b[^\"]*\\btext-(sm|xs)\\b|\\btext-sm\\b[^\"]*\\bfont-medium\\b/]",
                    message:
                        "Use <Text> (variant muted/caption/label) from @/components/typography instead of raw text-sm/text-xs/text-muted-foreground/font-medium classes.",
                },
                {
                    selector: "JSXOpeningElement[name.name=/^h[1-6]$/] > JSXAttribute[name.name='className']",
                    message:
                        "Style headings with <Heading variant=…> from @/components/typography, not classes on a raw <hN>.",
                },
            ],
        },
    },
    {
        // The rule that steers app callers toward <Text>/<Heading> must not fire where authoring raw
        // classes is legitimate: the primitive layer (shadcn/ui) and the typography components
        // themselves; the embeddable widget package, which ships its own token system (rq-*) and does
        // not depend on @/components/typography; and Storybook infra chrome.
        files: [
            "src/components/shadcn/ui/**/*.{ts,tsx}",
            "src/components/typography.tsx",
            "packages/widget/**/*.{ts,tsx}",
            ".storybook/**/*.{ts,tsx}",
        ],
        rules: {
            "no-restricted-syntax": "off",
        },
    },
    ...storybook.configs["flat/recommended"],
    {
        // Storybook config files are not part of the app's fast-refresh graph.
        files: [".storybook/**/*.{ts,tsx}"],
        rules: {
            "react-refresh/only-export-components": "off",
        },
    },
]);
