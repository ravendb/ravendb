import { RevisionsBinCleanerFormData } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import moment from "moment";
import RevisionsBinConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;

export const DEFAULT_CLEANER_FREQUENCY_IN_SEC = 300;
export const DEFAULT_MINIMUM_ENTRIES_AGE_TO_KEEP_IN_SEC = moment.duration(30, "days").asSeconds();

function mapToDto(dto: RevisionsBinCleanerFormData): RevisionsBinConfiguration {
    const minimumEntriesAgeToKeepInSec = dto.minimumEntriesAgeToKeep ?? DEFAULT_MINIMUM_ENTRIES_AGE_TO_KEEP_IN_SEC;

    return {
        Disabled: !dto.isRevisionsBinCleanerEnabled,
        MinimumEntriesAgeToKeepInMin: dto.isMinimumEntriesAgeToKeepEnabled
            ? moment.duration(minimumEntriesAgeToKeepInSec, "seconds").asMinutes()
            : 0,
        CleanerFrequencyInSec: dto.cleanerFrequencyInSec ?? DEFAULT_CLEANER_FREQUENCY_IN_SEC,
    };
}

function mapToFormData(dto: RevisionsBinConfiguration): RevisionsBinCleanerFormData {
    if (!dto) {
        return {
            isRevisionsBinCleanerEnabled: false,
            isMinimumEntriesAgeToKeepEnabled: false,
            minimumEntriesAgeToKeep: null,
            cleanerFrequencyInSec: null,
        };
    }

    const minimumEntriesAgeToKeepInSec = moment.duration(dto.MinimumEntriesAgeToKeepInMin ?? 0, "minutes").asSeconds();

    return {
        isRevisionsBinCleanerEnabled: !dto.Disabled,
        isMinimumEntriesAgeToKeepEnabled: dto.MinimumEntriesAgeToKeepInMin !== 0,
        minimumEntriesAgeToKeep: [0, DEFAULT_MINIMUM_ENTRIES_AGE_TO_KEEP_IN_SEC].includes(minimumEntriesAgeToKeepInSec)
            ? null
            : minimumEntriesAgeToKeepInSec,
        cleanerFrequencyInSec:
            dto.CleanerFrequencyInSec !== DEFAULT_CLEANER_FREQUENCY_IN_SEC ? (dto.CleanerFrequencyInSec ?? null) : null,
    };
}

export const revisionsBinCleanerUtils = {
    mapToDto,
    mapToFormData,
};
