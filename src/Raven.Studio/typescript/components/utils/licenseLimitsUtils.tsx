import { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";

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

export const featureAvailabilityProfessionalOrAbove: FeatureAvailabilityData[] = [
    {
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];

export const featureAvailabilityEnterprise: FeatureAvailabilityData[] = [
    {
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
