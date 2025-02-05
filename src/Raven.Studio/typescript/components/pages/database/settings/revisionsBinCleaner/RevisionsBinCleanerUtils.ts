import { RevisionsBinCleanerFormData } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import RevisionsBinConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;
import assertUnreachable from "components/utils/assertUnreachable";
import { SelectOption } from "components/common/select/Select";

function mapToDto(dto: RevisionsBinCleanerFormData): RevisionsBinConfiguration {
    return {
        Disabled: !dto.isRevisionsBinCleanerEnabled,
        MinimumEntriesAgeToKeepInMin: dto.isMinimumEntriesAgeToKeepEnabled
            ? timeMagnitudeToDto(dto.minimumEntriesAgeToKeepInMin, dto.timeMagnitude)
            : 0,
        RefreshFrequencyInSec: dto.isRefreshFrequencyEnabled ? dto.refreshFrequencyInSec : 300,
    };
}

const timeMagnitudeOptions: SelectOption<timeMagnitude>[] = ["minutes", "hours", "days"].map((x: timeMagnitude) => ({
    value: x,
    label: x,
}));

function timeMagnitudeToDto(timeInMinutesDto: number, timeMagnitude: timeMagnitude) {
    let timeInMinutes = timeInMinutesDto;
    switch (timeMagnitude) {
        case "minutes":
            return timeInMinutes;
        case "hours":
            return (timeInMinutes *= 60);
        case "days":
            return (timeInMinutes *= 24 * 60);
        default:
            assertUnreachable(timeMagnitude);
    }
}

function convertMinutesToTimeMagnitude(totalMinutes: number): {
    minimumEntriesAgeToKeepInMin: number;
    timeMagnitude: timeMagnitude;
} {
    if (totalMinutes >= 1440 && totalMinutes % 1440 === 0) {
        return { minimumEntriesAgeToKeepInMin: totalMinutes / 1440, timeMagnitude: "days" };
    }

    if (totalMinutes >= 60 && totalMinutes % 60 === 0) {
        return { minimumEntriesAgeToKeepInMin: totalMinutes / 60, timeMagnitude: "hours" };
    }

    return { minimumEntriesAgeToKeepInMin: totalMinutes, timeMagnitude: "minutes" };
}

function mapToFormData(dto: RevisionsBinConfiguration): RevisionsBinCleanerFormData {
    if (!dto) {
        return {
            isRevisionsBinCleanerEnabled: false,
            isMinimumEntriesAgeToKeepEnabled: false,
            timeMagnitude: "minutes",
            minimumEntriesAgeToKeepInMin: null,
            isRefreshFrequencyEnabled: false,
            refreshFrequencyInSec: null,
        };
    }

    const { minimumEntriesAgeToKeepInMin, timeMagnitude } = convertMinutesToTimeMagnitude(
        dto.MinimumEntriesAgeToKeepInMin
    );

    return {
        isRevisionsBinCleanerEnabled: !dto.Disabled,
        isMinimumEntriesAgeToKeepEnabled: dto.MinimumEntriesAgeToKeepInMin != null,
        minimumEntriesAgeToKeepInMin,
        timeMagnitude,
        isRefreshFrequencyEnabled: dto.RefreshFrequencyInSec !== 300,
        refreshFrequencyInSec: dto.RefreshFrequencyInSec !== 300 ? dto.RefreshFrequencyInSec : null,
    };
}

export const revisionsBinCleanerUtils = {
    mapToDto,
    mapToFormData,
    timeMagnitudeOptions,
};
