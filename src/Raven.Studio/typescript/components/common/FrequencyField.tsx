import moment from "moment";
import { Control, FieldPath, FieldValues } from "react-hook-form";
import { FormDurationPickerWithDefault } from "components/common/FieldWithDefault";
import LicenseLimitedFrequencyField from "components/common/LicenseLimitedFrequencyField";

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
    defaultFrequencyInSec: number;
    licenseLimit?: LicenseFrequencyLimit;
}

export default function FrequencyField<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    name,
    label,
    disabled,
    defaultFrequencyInSec,
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
        <div className="align-self-start mw-100">
            <FormDurationPickerWithDefault
                control={control}
                name={name}
                label={label}
                disabled={disabled}
                defaultTotalSeconds={defaultFrequencyInSec}
                showSeconds
            />
        </div>
    );
}

export function getLicenseFrequencyLimit({
    minPeriodInHours,
    isFeatureEnabled,
    frequencyInSec,
    defaultFrequencyInSec,
    limitMessage,
}: {
    minPeriodInHours: number;
    isFeatureEnabled: boolean;
    frequencyInSec: number;
    defaultFrequencyInSec: number;
    limitMessage: string;
}): LicenseFrequencyLimit {
    const isLicenseLimited = minPeriodInHours > 0;
    const minFrequencyInSec = moment.duration(minPeriodInHours, "hours").asSeconds();

    return {
        minPeriodInHours,
        minFrequencyInSec,
        isLicenseLimited,
        isBelowLimit:
            isLicenseLimited && isFeatureEnabled && (frequencyInSec ?? defaultFrequencyInSec) < minFrequencyInSec,
        limitMessage,
    };
}
