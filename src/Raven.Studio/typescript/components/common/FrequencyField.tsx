import moment from "moment";
import { Control, FieldPath, FieldValues } from "react-hook-form";
import { DEFAULT_ITEMS_TO_PROCESS } from "common/constants/maxItemsToProcess";
import { FormDurationPickerWithDefault } from "components/common/FieldWithDefault";
import LicenseLimitedFrequencyField from "components/common/LicenseLimitedFrequencyField";

const DEFAULT_FREQUENCY_IN_SEC = 60;

export interface LicenseFrequencyLimit {
    minPeriodInHours: number;
    minFrequencyInSec: number;
    isLicenseLimited: boolean;
    isBelowLimit: boolean;
    limitMessage: string;
}

interface FrequencyFieldProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    name: TName;
    label: string;
    disabled: boolean;
    defaultFrequencyInSec?: number;
    licenseLimit?: LicenseFrequencyLimit;
}

export default function FrequencyField<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    name,
    label,
    disabled,
    defaultFrequencyInSec = DEFAULT_FREQUENCY_IN_SEC,
    licenseLimit,
}: FrequencyFieldProps<TFieldValues, TName>) {
    if (licenseLimit?.isLicenseLimited) {
        return (
            <LicenseLimitedFrequencyField
                control={control}
                name={name}
                label={label}
                disabled={disabled}
                minPeriodInHours={licenseLimit.minPeriodInHours}
                limitMessage={licenseLimit.limitMessage}
                isBelowLimit={licenseLimit.isBelowLimit}
            />
        );
    }

    return (
        <FormDurationPickerWithDefault
            control={control}
            name={name}
            label={label}
            disabled={disabled}
            defaultTotalSeconds={defaultFrequencyInSec}
            showSeconds
        />
    );
}

export function getLicenseFrequencyLimit({
    minPeriodInHours,
    isFeatureEnabled,
    frequencyInSec,
    limitMessage,
}: {
    minPeriodInHours: number;
    isFeatureEnabled: boolean;
    frequencyInSec: number;
    limitMessage: string;
}): LicenseFrequencyLimit {
    const isLicenseLimited = minPeriodInHours > 0;
    const minFrequencyInSec = moment.duration(minPeriodInHours, "hours").asSeconds();

    return {
        minPeriodInHours,
        minFrequencyInSec,
        isLicenseLimited,
        isBelowLimit:
            isLicenseLimited && isFeatureEnabled && (frequencyInSec ?? DEFAULT_FREQUENCY_IN_SEC) < minFrequencyInSec,
        limitMessage,
    };
}

export function getFrequencySettingsOnToggle({
    isEnabled,
    frequencyInSec,
    maxItemsToProcess,
    licenseLimit,
}: {
    isEnabled: boolean;
    frequencyInSec: number;
    maxItemsToProcess: number;
    licenseLimit?: LicenseFrequencyLimit;
}): { frequencyInSec: number; maxItemsToProcess: number } {
    if (!isEnabled) {
        return { frequencyInSec: null, maxItemsToProcess: null };
    }

    const needsLicenseMinimum = licenseLimit?.isLicenseLimited && frequencyInSec == null;

    return {
        frequencyInSec: needsLicenseMinimum ? licenseLimit.minFrequencyInSec : frequencyInSec,
        maxItemsToProcess: maxItemsToProcess ?? DEFAULT_ITEMS_TO_PROCESS,
    };
}
