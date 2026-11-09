import { RevisionsBinCleanerFormData } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import moment from "moment";
import RevisionsBinConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;

export const DEFAULT_CLEANER_FREQUENCY_IN_SEC = 300;

function mapToDto(dto: RevisionsBinCleanerFormData): RevisionsBinConfiguration {
    return {
        Disabled: !dto.isRevisionsBinCleanerEnabled,
        MinimumEntriesAgeToKeepInMin: dto.isMinimumEntriesAgeToKeepEnabled
            ? moment.duration(dto.minimumEntriesAgeToKeep, "seconds").asMinutes()
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

    return {
        isRevisionsBinCleanerEnabled: !dto.Disabled,
        isMinimumEntriesAgeToKeepEnabled: dto.MinimumEntriesAgeToKeepInMin !== 0,
        minimumEntriesAgeToKeep: dto.MinimumEntriesAgeToKeepInMin
            ? moment.duration(dto.MinimumEntriesAgeToKeepInMin, "minutes").asSeconds() // minimumEntriesAgeToKeep are in minutes, so we need format minutes to seconds
            : null,
        cleanerFrequencyInSec:
            dto.CleanerFrequencyInSec !== DEFAULT_CLEANER_FREQUENCY_IN_SEC ? (dto.CleanerFrequencyInSec ?? null) : null,
    };
}

export const revisionsBinCleanerUtils = {
    mapToDto,
    mapToFormData,
};
