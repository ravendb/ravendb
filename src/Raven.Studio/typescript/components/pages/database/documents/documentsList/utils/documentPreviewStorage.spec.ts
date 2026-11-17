import versionProvider = require("common/versionProvider");
import { documentPreviewStorage } from "./documentPreviewStorage";

describe("documentPreviewStorage", () => {
    beforeEach(() => {
        localStorage.clear();
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    it("keeps the document preview on until it is turned off", () => {
        expect(documentPreviewStorage.isEnabled()).toBe(true);

        documentPreviewStorage.save(false);

        expect(documentPreviewStorage.isEnabled()).toBe(false);
    });

    it("keeps the document preview off after a Studio upgrade", () => {
        const studioVersion = jest.spyOn(versionProvider, "version", "get").mockReturnValue("7.2");
        documentPreviewStorage.save(false);

        studioVersion.mockReturnValue("7.3");

        expect(documentPreviewStorage.isEnabled()).toBe(false);
    });
});
