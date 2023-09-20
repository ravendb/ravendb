import { getLicenseAvailabilityType } from "components/utils/licenseLimitsUtils";
import { AvailabilityValue, FeatureAvailabilityData } from "../FeatureAvailabilitySummary";

interface GetLicenseAvailabilityDataProps {
    licenseType: Raven.Server.Commercial.LicenseType;
    overrideClusterLimit: number;
    overrideDatabaseLimit: number;
}

function shouldOverride(value: AvailabilityValue, overwriteValue: AvailabilityValue) {
    if (overwriteValue == null && value === Infinity) {
        return false;
    }
    if (overwriteValue === value) {
        return false;
    }

    return true;
}

function getLicenseAvailabilityData(props: GetLicenseAvailabilityDataProps): FeatureAvailabilityData[] {
    const { licenseType, overrideClusterLimit, overrideDatabaseLimit } = props;

    const featureAvailabilityData: FeatureAvailabilityData[] = [
        {
            featureName: "Limit per database",
            community: { value: 1 },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
        },
        {
            featureName: "Limit per cluster",
            community: { value: 5 },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
        },
    ];

    const type = getLicenseAvailabilityType(licenseType);
    if (!type) {
        return featureAvailabilityData;
    }

    const databaseLimitData = featureAvailabilityData[0][type];
    const clusterLimitData = featureAvailabilityData[1][type];

    if (shouldOverride(databaseLimitData.value, overrideDatabaseLimit)) {
        databaseLimitData.overwrittenValue = overrideDatabaseLimit;
    }
    if (shouldOverride(clusterLimitData.value, overrideClusterLimit)) {
        clusterLimitData.overwrittenValue = overrideClusterLimit;
    }

    return featureAvailabilityData;
}

export const databaseCustomSortersAndAnalyzersUtils = {
    getLicenseAvailabilityData,
};
