import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";

// Product vocabulary is a UX contract: one concept, one word, on every surface.
// See the Glossary section in AGENTS.md. This guard reads the AST and inspects only
// string literals and JSX text, so code comments describing the real CDC mechanism
// stay out of scope — the mechanism keeps its name, the labels do not.

const SRC_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const SCANNED_DIRECTORIES = ["pages", "components", "lib"];
const SCANNED_FILES = ["routes.tsx"];
const SKIPPED_DIRECTORIES = new Set(["generated", "mocks"]);

type BannedTerm = {
    /** Reported in the failure message. */
    name: string;
    pattern: RegExp;
    /** What to write instead. */
    use: string;
};

const BANNED_TERMS: BannedTerm[] = [
    // The negative lookahead exempts `application/json` MIME strings.
    { name: "application", pattern: /\bapplications?\b(?!\/json)/gi, use: '"app" / "apps"' },
    // Case-sensitive on purpose: `Cdc`-cased identifiers and `CDC_`-prefixed
    // constants never match, because `_` is a word character.
    { name: "CDC", pattern: /\bCDC\b/g, use: '"data source", or "sync" for the running pipeline' },
    // Does not match `ApplianceAppResponse` or `ApplianceOptions` — no word boundary inside them.
    { name: "appliance", pattern: /\bappliance\b/gi, use: '"Quill"' },
    { name: "billing", pattern: /\bbilling\b/gi, use: "nothing — Quill has no billing" },
    // Bare "operator" is deliberately allowed: the role concept is kept, only the
    // labels that conflate the role with the credential are not.
    { name: "operator key", pattern: /operator\s+(?:API\s+)?key/gi, use: '"Dashboard API key"' },
];

type Allowance = {
    /** Path relative to `src/`, forward slashes. */
    file: string;
    term: string;
    reason: string;
};

// An entry exempts a whole file for one term, not one specific line. That is a
// deliberate simplification — pinning line numbers would make the allowlist churn on
// every unrelated edit — but it does mean a new violation of the same term in an
// allowlisted file goes unreported. Keep entries rare and narrow.
const ALLOWED: Allowance[] = [
    {
        file: "pages/setup/add-app-wizard/steps/verify/verify-schema-columns.tsx",
        term: "CDC",
        reason:
            "Names the PostgreSQL / SQL Server feature the operator must enable. " +
            "Rewording it leaves the message unactionable.",
    },
];

function isAllowed(file: string, term: string) {
    return ALLOWED.some((allowance) => allowance.file === file && allowance.term === term);
}

function collectSourceFiles(directory: string, found: string[]) {
    for (const entry of readdirSync(directory)) {
        const full = path.join(directory, entry);

        if (statSync(full).isDirectory()) {
            if (!SKIPPED_DIRECTORIES.has(entry)) {
                collectSourceFiles(full, found);
            }
            continue;
        }

        // Stories and tests assert on the very strings under review, so scanning them
        // would double-report every finding.
        if (/\.tsx?$/.test(entry) && !/\.(test|stories)\.tsx?$/.test(entry)) {
            found.push(full);
        }
    }

    return found;
}

function findUserFacingText(sourceFile: ts.SourceFile) {
    const texts: Array<{ text: string; line: number }> = [];

    const visit = (node: ts.Node) => {
        const isUserFacingText =
            ts.isStringLiteral(node) ||
            ts.isNoSubstitutionTemplateLiteral(node) ||
            ts.isTemplateHead(node) ||
            ts.isTemplateMiddle(node) ||
            ts.isTemplateTail(node) ||
            ts.isJsxText(node);

        if (isUserFacingText) {
            texts.push({
                text: node.text,
                line: sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile)).line + 1,
            });
        }

        ts.forEachChild(node, visit);
    };

    visit(sourceFile);

    return texts;
}

function findViolations() {
    const files = [
        ...SCANNED_DIRECTORIES.flatMap((directory) => collectSourceFiles(path.join(SRC_DIR, directory), [])),
        ...SCANNED_FILES.map((file) => path.join(SRC_DIR, file)),
    ];

    const violations: string[] = [];

    for (const file of files) {
        const relativePath = path.relative(SRC_DIR, file).split(path.sep).join("/");
        const sourceFile = ts.createSourceFile(
            file,
            readFileSync(file, "utf8"),
            ts.ScriptTarget.Latest,
            true,
            ts.ScriptKind.TSX,
        );

        for (const { text, line } of findUserFacingText(sourceFile)) {
            for (const term of BANNED_TERMS) {
                if (isAllowed(relativePath, term.name)) {
                    continue;
                }

                term.pattern.lastIndex = 0;
                let match: RegExpExecArray | null;

                while ((match = term.pattern.exec(text)) !== null) {
                    violations.push(`${relativePath}:${line} says "${match[0]}" — use ${term.use}`);
                }
            }
        }
    }

    return violations;
}

describe("product vocabulary", () => {
    it("keeps retired terms out of user-facing text", () => {
        expect(findViolations()).toEqual([]);
    });

    it("scans a meaningful number of files", () => {
        // Guards against a silently-broken walk reporting zero violations because it
        // found zero files.
        const files = SCANNED_DIRECTORIES.flatMap((directory) => collectSourceFiles(path.join(SRC_DIR, directory), []));

        expect(files.length).toBeGreaterThan(100);
    });
});
