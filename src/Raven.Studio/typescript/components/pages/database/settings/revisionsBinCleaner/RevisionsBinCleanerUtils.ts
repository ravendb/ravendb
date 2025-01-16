import { RevisionsBinCleanerFormData } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import RevisionsBinConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;

export function mapToDto(dto: RevisionsBinCleanerFormData): RevisionsBinConfiguration {
    return {
        Disabled: !dto.isRevisionsBinCleanerEnabled,
        MinimumEntriesAgeToKeepInMin: dto.isMinimumEntriesAgeToKeepEnabled
            ? dto.minimumEntriesAgeToKeepInMin
            : null,
        RefreshFrequencyInSec: dto.isRefreshFrequencyEnabled ? dto.refreshFrequencyInSec : 300,
    };
}

export function mapToFormData(dto: RevisionsBinConfiguration): RevisionsBinCleanerFormData {
    if (!dto) {
        return {
            isRevisionsBinCleanerEnabled: false,
            isMinimumEntriesAgeToKeepEnabled: false,
            minimumEntriesAgeToKeepInMin: null,
            isRefreshFrequencyEnabled: false,
            refreshFrequencyInSec: null,
        };
    }

    return {
        isRevisionsBinCleanerEnabled: !dto.Disabled,
        isMinimumEntriesAgeToKeepEnabled: dto.MinimumEntriesAgeToKeepInMin != null,
        minimumEntriesAgeToKeepInMin: dto.MinimumEntriesAgeToKeepInMin,
        isRefreshFrequencyEnabled: dto.RefreshFrequencyInSec !== 300,
        refreshFrequencyInSec: dto.RefreshFrequencyInSec,
    };
}
