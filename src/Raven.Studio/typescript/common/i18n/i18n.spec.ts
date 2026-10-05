import { changeLanguage, createTranslator, currentLanguage, i18n, initI18n, loadLanguage } from "./i18n";
import { en, languageLoaders, StudioLanguage, supportedLanguages, TranslationNamespace } from "./resources";

const pluralSuffixRegex = /_(zero|one|two|few|many|other)$/;
const placeholderRegex = /\{\{[^}]*\}\}/g;
const markupRegex = /\{\{[^}]*\}\}|<\/?\w+\s*\/?>/g;

function flattenEntries(value: unknown, prefix = ""): [key: string, value: string][] {
    if (typeof value === "string") {
        return [[prefix, value]];
    }
    return Object.entries(value as Record<string, unknown>).flatMap(([key, nested]) =>
        flattenEntries(nested, prefix ? `${prefix}.${key}` : key)
    );
}

function markupOf(value: string): string[] {
    return (value.match(markupRegex) ?? []).sort();
}

function markupMismatches(enBundle: unknown, languageBundle: unknown): string[] {
    const enValues = new Map(flattenEntries(enBundle));

    return flattenEntries(languageBundle)
        .filter(([key, value]) => {
            const enValue = enValues.get(key) ?? enValues.get(key.replace(pluralSuffixRegex, "_other"));
            return enValue !== undefined && markupOf(value).join() !== markupOf(enValue).join();
        })
        .map(([key]) => key);
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

    it("switches to English when asked for a language it does not support", async () => {
        await changeLanguage("pl");
        await changeLanguage("de" as StudioLanguage);
        expect(i18n.language).toBe("en");
    });

    it("keeps the latest requested language when an earlier one loads slower", async () => {
        const plResources = await languageLoaders.pl();
        Object.keys(en).forEach((ns) => i18n.removeResourceBundle("pl", ns));
        let finishLoadingPl: () => void;
        const plLoader = jest
            .spyOn(languageLoaders, "pl")
            .mockReturnValue(new Promise((resolve) => (finishLoadingPl = () => resolve(plResources))));

        const plChange = changeLanguage("pl");
        await changeLanguage("en");
        finishLoadingPl();
        await plChange;

        expect(i18n.language).toBe("en");
        plLoader.mockRestore();
    });

    it("exposes the current language to Knockout", async () => {
        const changes: string[] = [];
        const subscription = currentLanguage.subscribe((language) => changes.push(language));

        await changeLanguage("pl");

        expect(changes).toEqual(["pl"]);
        subscription.dispose();
    });

    it("initI18n is idempotent", () => {
        const first = initI18n();
        const second = initI18n();
        expect(second).toBe(first);
    });

    describe("key parity", () => {
        const namespaces = Object.keys(en) as TranslationNamespace[];

        function keysOf(language: StudioLanguage, ns: TranslationNamespace): string[] {
            return flattenEntries(i18n.getResourceBundle(language, ns)).map(([key]) => key);
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

        it("compares placeholders and tags regardless of their order", () => {
            expect(markupMismatches({ a: "<b>{{x}}</b> {{y}}" }, { a: "{{y}} <b>{{x}}</b>" })).toEqual([]);
            expect(markupMismatches({ a: "Hi {{documentId}}" }, { a: "Hi {{docId}}" })).toEqual(["a"]);
            expect(markupMismatches({ a: "<strong>Hi</strong><br/>" }, { a: "Hi<br/>" })).toEqual(["a"]);
            expect(markupMismatches({ a_other: "{{count}} items" }, { a_few: "{{count}} elementy" })).toEqual([]);
        });

        it("en uses only plain {{name}} placeholders", () => {
            const placeholders = namespaces.flatMap((ns) =>
                flattenEntries(en[ns]).flatMap(([, value]) => [...value.matchAll(placeholderRegex)].map(([match]) => match))
            );
            expect(placeholders.filter((placeholder) => !/^\{\{\w+\}\}$/.test(placeholder))).toEqual([]);
        });

        supportedLanguages.forEach((language) => {
            namespaces.forEach((ns) => {
                if (language !== "en") {
                    it(`${language}/${ns} has the same base keys as en/${ns}`, () => {
                        expect(baseKeys(keysOf(language, ns))).toEqual(baseKeys(keysOf("en", ns)));
                    });

                    it(`${language}/${ns} keeps the placeholders and tags of en/${ns}`, () => {
                        expect(
                            markupMismatches(i18n.getResourceBundle("en", ns), i18n.getResourceBundle(language, ns))
                        ).toEqual([]);
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
        const commonT = createTranslator("common");
        const sorterT = createTranslator("editCustomSorter");

        it("translates its own namespace", () => {
            expect(t("noConflicts")).toBe("No conflicts found.");
            expect(commonT("save")).toBe("Save");
        });

        it("follows the current language", async () => {
            await changeLanguage("pl");
            expect(commonT("save")).toBe("Zapisz");
        });

        it("re-evaluates Knockout computeds on language change", async () => {
            const label = ko.computed(() => commonT("save"));

            await changeLanguage("pl");

            expect(label()).toBe("Zapisz");
            label.dispose();
        });

        it("escapes interpolated values", () => {
            expect(t("resolvingConflictFor", { documentId: "<img>" })).toBe("Resolving conflict for: &lt;img&gt;");
        });

        it("resolves context variants", () => {
            expect(sorterT("heading", { context: "new" })).toBe("New Custom Sorter");
        });

        it("accepts only keys of its own namespace", () => {
            // @ts-expect-error key from another namespace
            expect(t("common:save")).toBe("Save");
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

            const notFound = true as boolean;
            // @ts-expect-error a union of keys needs the options of each of them
            expect(t(notFound ? "documentNotFound" : "noConflicts")).toContain("{{documentId}}");
            expect(t(notFound ? "documentNotFound" : "noConflicts", { documentId: "orders" })).toContain("orders");
        });
    });
});
