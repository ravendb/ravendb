import {
    AvailabilityValue,
    FeatureAvailabilityData,
    FeatureAvailabilityFootnote,
} from "components/common/FeatureAvailabilitySummary";
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

export type LicenseAvailabilityType = "community" | "professional" | "enterprise" | "enterpriseAi" | "quill";

export function getLicenseAvailabilityType(licenseType: Raven.Server.Commercial.LicenseType): LicenseAvailabilityType {
    switch (licenseType) {
        case "Quill":
            return "quill";
        case "Essential":
        case "Community":
            return "community";
        case "Professional":
            return "professional";
        case "Enterprise":
            return "enterprise";
        case "Developer":
        case "EnterpriseAi":
            return "enterpriseAi";
        default:
            return null;
    }
}

function useLicenseAvailability(
    featureAvailabilityData: FeatureAvailabilityData[],
    isFeatureInLicense: boolean
): FeatureAvailabilityData[] {
    const data = structuredClone(featureAvailabilityData);
    const licenseType = useAppSelector(licenseSelectors.licenseType);

    const type = getLicenseAvailabilityType(licenseType);
    if (!type) {
        return data;
    }

    const valueData = data[0][type];

    if (valueData.value !== isFeatureInLicense) {
        valueData.overwrittenValue = isFeatureInLicense;
    }

    return data;
}

export function useProfessionalOrAboveLicenseAvailability(isFeatureInLicense: boolean): FeatureAvailabilityData[] {
    const featureAvailabilityData: FeatureAvailabilityData[] = [
        {
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
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
            quill: { value: true },
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
        footnote?: FeatureAvailabilityFootnote;
    }[];
}

export function useLimitedFeatureAvailability({
    defaultFeatureAvailability,
    overwrites,
}: UseLimitedFeatureAvailabilityProps) {
    const licenseType = useAppSelector(licenseSelectors.licenseType);

    const featureAvailability: FeatureAvailabilityData[] = defaultFeatureAvailability.map((x) => ({
        ...x,
        enterpriseAi: x.enterpriseAi ?? structuredClone(x.enterprise),
    }));

    const type = getLicenseAvailabilityType(licenseType);
    if (!type) {
        return featureAvailability;
    }

    for (const overwrite of overwrites) {
        const data = featureAvailability.find((x) => x.featureName === overwrite.featureName);
        const valueData = { ...data[type] };

        if (shouldOverrideLicenseAvailability(valueData.value, overwrite.value)) {
            valueData.overwrittenValue = overwrite.value;
        }
        if (overwrite.footnote) {
            valueData.footnote = overwrite.footnote;
        }

        data[type] = valueData;
    }

    return featureAvailability;
}
