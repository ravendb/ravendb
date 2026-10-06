import studioSettings = require("common/settings/studioSettings");
import globalSettings = require("common/settings/globalSettings");

describe("globalSettings", () => {
    afterEach(() => {
        localStorage.removeItem(globalSettings.storageKey);
    });

    it("has a local language setting defaulting to en", async () => {
        const settings = await studioSettings.default.globalSettings(true);

        expect(settings.language.saveLocation).toBe("local");
        expect(settings.language.getValue()).toBe("en");
    });

    it("reads the stored language before settings are loaded", () => {
        localStorage.setItem(globalSettings.storageKey, JSON.stringify({ language: "pl" }));

        expect(globalSettings.readStoredLanguage()).toBe("pl");
    });

    it("falls back to en when nothing valid is stored", () => {
        expect(globalSettings.readStoredLanguage()).toBe("en");

        localStorage.setItem(globalSettings.storageKey, JSON.stringify({ language: "de" }));

        expect(globalSettings.readStoredLanguage()).toBe("en");

        localStorage.setItem(globalSettings.storageKey, "{not json");

        expect(globalSettings.readStoredLanguage()).toBe("en");
    });
});
