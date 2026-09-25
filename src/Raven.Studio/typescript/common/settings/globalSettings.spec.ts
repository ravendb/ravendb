import studioSettings = require("common/settings/studioSettings");

describe("globalSettings", () => {
    it("has a local language setting defaulting to en", async () => {
        const settings = await studioSettings.default.globalSettings(true);

        expect(settings.language.saveLocation).toBe("local");
        expect(settings.language.getValue()).toBe("en");
    });
});
