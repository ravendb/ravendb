import {
    DEFAULT_MINIMUM_ENTRIES_AGE_TO_KEEP_IN_SEC,
    revisionsBinCleanerUtils,
} from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerUtils";

const formData = {
    isRevisionsBinCleanerEnabled: true,
    isMinimumEntriesAgeToKeepEnabled: true,
    minimumEntriesAgeToKeep: null as number,
    cleanerFrequencyInSec: null as number,
};

describe("revisionsBinCleanerUtils.mapToDto", () => {
    it("sends the default minimum entries age when the field is empty", () => {
        const dto = revisionsBinCleanerUtils.mapToDto(formData);

        expect(dto.MinimumEntriesAgeToKeepInMin).toBe(DEFAULT_MINIMUM_ENTRIES_AGE_TO_KEEP_IN_SEC / 60);
    });

    it("sends the custom minimum entries age in minutes", () => {
        const dto = revisionsBinCleanerUtils.mapToDto({ ...formData, minimumEntriesAgeToKeep: 7200 });

        expect(dto.MinimumEntriesAgeToKeepInMin).toBe(120);
    });

    it("sends 0 when 'set minimum entries age to keep' is off", () => {
        const dto = revisionsBinCleanerUtils.mapToDto({ ...formData, isMinimumEntriesAgeToKeepEnabled: false });

        expect(dto.MinimumEntriesAgeToKeepInMin).toBe(0);
    });
});
