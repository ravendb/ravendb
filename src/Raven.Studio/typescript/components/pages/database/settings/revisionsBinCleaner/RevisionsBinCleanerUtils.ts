import { RevisionsBinCleanerFormData } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import moment from "moment";
import RevisionsBinConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;

function mapToDto(dto: RevisionsBinCleanerFormData): RevisionsBinConfiguration {
    return {
        Disabled: !dto.isRevisionsBinCleanerEnabled,
        MinimumEntriesAgeToKeepInMin: dto.isMinimumEntriesAgeToKeepEnabled
            ? moment.duration(dto.minimumEntriesAgeToKeep, "seconds").asMinutes()
            : 0,
        CleanerFrequencyInSec: dto.isCleanerFrequencyInSecEnabled ? dto.cleanerFrequencyInSec : 300,
    };
}

function mapToFormData(dto: RevisionsBinConfiguration): RevisionsBinCleanerFormData {
    if (!dto) {
        return {
            isRevisionsBinCleanerEnabled: false,
            isMinimumEntriesAgeToKeepEnabled: false,
            minimumEntriesAgeToKeep: null,
            isCleanerFrequencyInSecEnabled: false,
            cleanerFrequencyInSec: null,
        };
    }

    return {
        isRevisionsBinCleanerEnabled: !dto.Disabled,
        isMinimumEntriesAgeToKeepEnabled: dto.MinimumEntriesAgeToKeepInMin !== 0,
        minimumEntriesAgeToKeep: dto.MinimumEntriesAgeToKeepInMin
            ? moment.duration(dto.MinimumEntriesAgeToKeepInMin, "minutes").asSeconds() // minimumEntriesAgeToKeep are in minutes, so we need format minutes to seconds
            : null,
        isCleanerFrequencyInSecEnabled: dto.CleanerFrequencyInSec !== 300,
        cleanerFrequencyInSec: dto.CleanerFrequencyInSec !== 300 ? dto.CleanerFrequencyInSec : null,
    };
}

export const revisionsBinCleanerUtils = {
    mapToDto,
    mapToFormData,
};
