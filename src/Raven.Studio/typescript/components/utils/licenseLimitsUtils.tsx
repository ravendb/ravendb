import { AvailabilityValue, FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";

export type LicenseLimitReachStatus = "notReached" | "closeToLimit" | "limitReached";

function calculateThreshold(limit: number): number {
    return Math.max(Math.floor(0.8 * limit), limit - 8);
}

export function getLicenseLimitReachStatus(count: number, limit: number): LicenseLimitReachStatus {
    if (!count || !limit) {
        return "notReached";
    }

    if (count >= limit) {
        return "limitReached";
    }

    if (count >= calculateThreshold(limit)) {
        return "closeToLimit";
    }

    return "notReached";
}

export type LicenseAvailabilityType = "community" | "professional" | "enterprise";

export function getLicenseAvailabilityType(licenseType: Raven.Server.Commercial.LicenseType): LicenseAvailabilityType {
    switch (licenseType) {
        case "Essential":
        case "Community":
            return "community";
        case "Professional":
            return "professional";
        case "Enterprise":
            return "enterprise";
        default:
            return null;
    }
}
// // TODO remove + run webpack
// export const featureAvailabilityProfessionalOrAbove: FeatureAvailabilityData[] = [
//     {
//         community: { value: false },
//         professional: { value: true },
//         enterprise: { value: true },
//     },
// ];

// // TODO remove + run webpack
// export const featureAvailabilityEnterprise: FeatureAvailabilityData[] = [
//     {
//         community: { value: false },
//         professional: { value: false },
//         enterprise: { value: true },
//     },
// ];

function useLicenseAvailability(
    featureAvailabilityData: FeatureAvailabilityData[],
    isFeatureInLicense: boolean
): FeatureAvailabilityData[] {
    const licenseType = useAppSelector(licenseSelectors.licenseType);

    const type = getLicenseAvailabilityType(licenseType);
    if (!type) {
        return featureAvailabilityData;
    }

    const data = featureAvailabilityData[0][type];

    if (data.value !== isFeatureInLicense) {
        data.overwrittenValue = isFeatureInLicense;
    }

    return featureAvailabilityData;
}

export function useProfessionalOrAboveLicenseAvailability(isFeatureInLicense: boolean): FeatureAvailabilityData[] {
    const featureAvailabilityData: FeatureAvailabilityData[] = [
        {
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
        },
    ];

    return useLicenseAvailability(featureAvailabilityData, isFeatureInLicense);
}

export function useEnterpriseLicenseAvailability(isFeatureInLicense: boolean): FeatureAvailabilityData[] {
    const featureAvailabilityData: FeatureAvailabilityData[] = [
        {
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
        },
    ];

    return useLicenseAvailability(featureAvailabilityData, isFeatureInLicense);
}

export function shouldOverrideLicenseAvailability(value: AvailabilityValue, overwriteValue: AvailabilityValue) {
    if (overwriteValue == null && value === Infinity) {
        return false;
    }
    if (overwriteValue === value) {
        return false;
    }

    return true;
}

interface UseLimitedFeatureAvailabilityProps {
    defaultFeatureAvailability: FeatureAvailabilityData[];
    overwrites: {
        featureName: string;
        value: AvailabilityValue;
    }[];
}

export function useLimitedFeatureAvailability({
    defaultFeatureAvailability,
    overwrites,
}: UseLimitedFeatureAvailabilityProps) {
    const licenseType = useAppSelector(licenseSelectors.licenseType);

    const featureAvailability = _.cloneDeep(defaultFeatureAvailability);

    const type = getLicenseAvailabilityType(licenseType);
    if (!type) {
        return featureAvailability;
    }

    for (const overwrite of overwrites) {
        const data = featureAvailability.find((x) => x.featureName === overwrite.featureName);

        if (shouldOverrideLicenseAvailability(data[type].value, overwrite.value)) {
            data[type].overwrittenValue = overwrite.value;
        }
    }

    return featureAvailability;
}
