import studioSettings = require("common/settings/studioSettings");
import globalSettings = require("common/settings/globalSettings");

describe("globalSettings", () => {
    beforeEach(() => {
        jest.spyOn(Storage.prototype, "setObject").mockImplementation(function (this: Storage, key: string, value: unknown) {
            this.setItem(key, JSON.stringify(value));
        });
    });

    afterEach(async () => {
        jest.restoreAllMocks();
        localStorage.removeItem(globalSettings.storageKey);
        const settings = await studioSettings.default.globalSettings();
        settings.language.setValueLazy("en");
    });

    it("has a local language setting defaulting to en", async () => {
        const settings = await studioSettings.default.globalSettings(true);

        expect(settings.language.saveLocation).toBe("local");
        expect(settings.language.getValue()).toBe("en");
    });

    it("reads the language saved through the settings before they are loaded", async () => {
        const settings = await studioSettings.default.globalSettings(true);

        await settings.language.setValue("pl");

        expect(globalSettings.readStoredLanguage()).toBe("pl");
    });

    it("falls back to en when nothing valid is stored", () => {
        expect(globalSettings.readStoredLanguage()).toBe("en");

        localStorage.setItem(globalSettings.storageKey, JSON.stringify({ language: JSON.stringify("de") }));

        expect(globalSettings.readStoredLanguage()).toBe("en");

        localStorage.setItem(globalSettings.storageKey, JSON.stringify({ language: "pl" }));

        expect(globalSettings.readStoredLanguage()).toBe("en");

        localStorage.setItem(globalSettings.storageKey, "{not json");

        expect(globalSettings.readStoredLanguage()).toBe("en");
    });
});
