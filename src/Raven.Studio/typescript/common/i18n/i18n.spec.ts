import { changeLanguage, createTranslator, i18n, initI18n, loadLanguage } from "./i18n";
import { en, StudioLanguage, supportedLanguages, TranslationNamespace } from "./resources";

const pluralSuffixRegex = /_(zero|one|two|few|many|other)$/;

function flattenKeys(value: unknown, prefix = ""): string[] {
    if (value === null || typeof value !== "object") {
        return [prefix];
    }
    return Object.entries(value as Record<string, unknown>).flatMap(([key, nested]) =>
        flattenKeys(nested, prefix ? `${prefix}.${key}` : key)
    );
}

function baseKeys(keys: string[]): string[] {
    return [...new Set(keys.map((key) => key.replace(pluralSuffixRegex, "")))].sort();
}

function missingPluralForms(language: string, enKeys: string[], languageKeys: string[]): string[] {
    const categories = new Intl.PluralRules(language).resolvedOptions().pluralCategories;
    const pluralBaseKeys = baseKeys(enKeys.filter((key) => pluralSuffixRegex.test(key)));

    return pluralBaseKeys
        .flatMap((base) => categories.map((category) => `${base}_${category}`))
        .filter((key) => !languageKeys.includes(key));
}

describe("i18n", () => {
    beforeAll(() => {
        initI18n();
    });

    afterEach(async () => {
        await changeLanguage("en");
    });

    it("starts in English", () => {
        expect(i18n.language).toBe("en");
        expect(i18n.t("common:save")).toBe("Save");
    });

    it("loads and switches language", async () => {
        await changeLanguage("pl");
        expect(i18n.t("common:save")).toBe("Zapisz");
    });

    it("falls back to English for unsupported language", async () => {
        await i18n.changeLanguage("de");
        expect(i18n.t("common:save")).toBe("Save");
    });

    it("initI18n is idempotent", () => {
        const first = initI18n();
        const second = initI18n();
        expect(second).toBe(first);
    });

    describe("key parity", () => {
        const namespaces = Object.keys(en) as TranslationNamespace[];

        function keysOf(language: StudioLanguage, ns: TranslationNamespace): string[] {
            return flattenKeys(i18n.getResourceBundle(language, ns));
        }

        beforeAll(async () => {
            await Promise.all(supportedLanguages.map(loadLanguage));
        });

        it("requires every plural form of the language", () => {
            expect(missingPluralForms("pl", ["items_one", "items_other"], ["items_one", "items_other"])).toEqual([
                "items_few",
                "items_many",
            ]);
        });

        supportedLanguages.forEach((language) => {
            namespaces.forEach((ns) => {
                if (language !== "en") {
                    it(`${language}/${ns} has the same base keys as en/${ns}`, () => {
                        expect(baseKeys(keysOf(language, ns))).toEqual(baseKeys(keysOf("en", ns)));
                    });
                }

                it(`${language}/${ns} has every plural form of ${language}`, () => {
                    expect(missingPluralForms(language, keysOf("en", ns), keysOf(language, ns))).toEqual([]);
                });
            });
        });
    });

    describe("createTranslator", () => {
        const t = createTranslator("conflicts");
        const sorterT = createTranslator("editCustomSorter");

        it("translates its own namespace and common keys", () => {
            expect(t("noConflicts")).toBe("No conflicts found.");
            expect(t("common:save")).toBe("Save");
        });

        it("follows the current language", async () => {
            await changeLanguage("pl");
            expect(t("common:save")).toBe("Zapisz");
        });

        it("escapes interpolated values", () => {
            expect(t("resolvingConflictFor", { documentId: "<img>" })).toBe("Resolving conflict for: &lt;img&gt;");
        });

        it("resolves context variants", () => {
            expect(sorterT("heading", { context: "new" })).toBe("New Custom Sorter");
        });

        it("rejects at compile time what renders wrong at runtime", () => {
            // @ts-expect-error unknown key
            expect(() => t("doesNotExist")).toThrow("Missing translation key");
            // @ts-expect-error missing interpolation options
            expect(t("documentNotFound")).toContain("{{documentId}}");
            // @ts-expect-error misspelled interpolation option
            expect(t("documentNotFound", { docId: "orders/1" })).toContain("{{documentId}}");
            // @ts-expect-error missing context for a key that has only context variants
            expect(() => sorterT("heading")).toThrow("Missing translation key");
            // @ts-expect-error unknown context
            expect(() => sorterT("heading", { context: "clone" })).toThrow("Missing translation key");
        });
    });
});
