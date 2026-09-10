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

function keyExists(fullKey: string): boolean {
    if (i18n.exists(fullKey)) {
        return true;
    }

    // context / plural variants ("heading_new", "items_other") have no base key of their own
    const [ns, key] = fullKey.split(":");
    const bundle = i18n.getResourceBundle("en", ns) as Record<string, unknown> | undefined;
    if (!bundle) {
        return false;
    }

    const segments = key.split(".");
    const leaf = segments[segments.length - 1];
    const parent = segments.slice(0, -1).reduce<unknown>((node, segment) => {
        return node && typeof node === "object" ? (node as Record<string, unknown>)[segment] : undefined;
    }, bundle);

    if (!parent || typeof parent !== "object") {
        return false;
    }

    return Object.keys(parent).some((candidate) => candidate.startsWith(leaf + "_"));
}

describe("Knockout views i18n keys", () => {
    beforeAll(() => {
        initI18n();
    });

    it("has at least one translated view", () => {
        const translatedViews = listHtmlFiles(viewsRoot).filter((file) =>
            i18nBindingRegex.test(fs.readFileSync(file, "utf8"))
        );
        expect(translatedViews.length).toBeGreaterThan(0);
    });

    it("every key used in a view exists in en resources", () => {
        const missing: string[] = [];

        for (const file of listHtmlFiles(viewsRoot)) {
            const html = fs.readFileSync(file, "utf8");
            for (const binding of html.matchAll(i18nBindingRegex)) {
                for (const match of binding[1].matchAll(keyRegex)) {
                    const key = match[1];
                    if (!keyExists(key)) {
                        missing.push(`${path.relative(viewsRoot, file)}: ${key}`);
                    }
                }
            }
        }

        expect(missing).toEqual([]);
    });
});
