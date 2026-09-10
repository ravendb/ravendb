import { i18n, initI18n } from "./i18n";
import { resources, supportedLanguages } from "./resources";

describe("i18n", () => {
    beforeAll(() => {
        initI18n();
    });

    afterEach(async () => {
        await i18n.changeLanguage("en");
    });

    it("starts in English", () => {
        expect(i18n.language).toBe("en");
        expect(i18n.t("common:save")).toBe("Save");
    });

    it("switches language", async () => {
        await i18n.changeLanguage("pl");
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
        const namespaces = Object.keys(resources.en) as (keyof typeof resources.en)[];

        function flattenKeys(value: unknown, prefix = ""): string[] {
            if (value === null || typeof value !== "object") {
                return [prefix];
            }
            return Object.entries(value as Record<string, unknown>).flatMap(([key, nested]) =>
                flattenKeys(nested, prefix ? `${prefix}.${key}` : key)
            );
        }

        supportedLanguages
            .filter((lng) => lng !== "en")
            .forEach((lng) => {
                namespaces.forEach((ns) => {
                    it(`${lng}/${ns}.json has the same keys as en/${ns}.json`, () => {
                        const enKeys = flattenKeys(resources.en[ns]).sort();
                        const otherKeys = flattenKeys(resources[lng][ns]).sort();
                        expect(otherKeys).toEqual(enKeys);
                    });
                });
            });
    });
});
