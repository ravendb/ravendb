import fs from "fs";
import path from "path";
import { i18n, initI18n } from "./i18n";

const viewsRoot = path.resolve(__dirname, "../../../wwwroot/App/views");

function listHtmlFiles(dir: string): string[] {
    return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
            return listHtmlFiles(fullPath);
        }
        return entry.name.endsWith(".html") ? [fullPath] : [];
    });
}

// data-bind attributes that use the i18n / i18nAttr bindings, and 'namespace:key' strings inside them
const i18nBindingRegex = /data-bind="([^"]*\bi18n(?:Attr)?\s*:[^"]*)"/g;
const keyRegex = /'([A-Za-z][\w]*:[\w.]+)'/g;
const contextRegex = /\bcontext\s*:([^,}]*)/g;
const stringLiteralRegex = /'([^']*)'/g;

function contextsOf(binding: string): string[] {
    return [...binding.matchAll(contextRegex)].flatMap(([, expression]) =>
        [...expression.matchAll(stringLiteralRegex)].map(([, context]) => context)
    );
}

function keyResolves(key: string, contexts: string[]): boolean {
    if (contexts.length === 0) {
        return i18n.exists(key);
    }
    return contexts.every((context) => i18n.exists(key, { context }));
}

function unresolvedKeys(html: string): string[] {
    return [...html.matchAll(i18nBindingRegex)].flatMap(([, binding]) => {
        const contexts = contextsOf(binding);
        return [...binding.matchAll(keyRegex)].map(([, key]) => key).filter((key) => !keyResolves(key, contexts));
    });
}

describe("Knockout views i18n keys", () => {
    beforeAll(() => {
        initI18n();
    });

    it("has at least one translated view", () => {
        const translatedViews = listHtmlFiles(viewsRoot).filter((file) =>
            fs.readFileSync(file, "utf8").match(i18nBindingRegex)
        );
        expect(translatedViews.length).toBeGreaterThan(0);
    });

    it("accepts context variants only for contexts passed in the binding", () => {
        const withContext = `<h3 data-bind="i18n: { key: 'editCustomSorter:heading', options: { context: isNew() ? 'new' : 'edit' } }"></h3>`;
        const withoutContext = `<h3 data-bind="i18n: 'editCustomSorter:heading'"></h3>`;
        const withUnknownContext = `<h3 data-bind="i18n: { key: 'editCustomSorter:heading', options: { context: 'clone' } }"></h3>`;

        expect(unresolvedKeys(withContext)).toEqual([]);
        expect(unresolvedKeys(withoutContext)).toEqual(["editCustomSorter:heading"]);
        expect(unresolvedKeys(withUnknownContext)).toEqual(["editCustomSorter:heading"]);
    });

    it("every key used in a view exists in en resources", () => {
        const missing = listHtmlFiles(viewsRoot).flatMap((file) =>
            unresolvedKeys(fs.readFileSync(file, "utf8")).map((key) => `${path.relative(viewsRoot, file)}: ${key}`)
        );

        expect(missing).toEqual([]);
    });
});
