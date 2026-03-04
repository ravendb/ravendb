import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import LicenseLimitsUsage = Raven.Server.Commercial.LicenseLimitsUsage;
import LicenseType = Raven.Server.Commercial.LicenseType;

interface LicenseState {
    status: LicenseStatus;
    support: Raven.Server.Commercial.LicenseSupportInfo;
    limitsUsage: LicenseLimitsUsage;
}

const initialState: LicenseState = {
    status: null,
    support: {
        Status: "NoSupport",
        EndsAt: undefined,
        SupportType: "None",
    },
    limitsUsage: {
        NumberOfStaticIndexesInCluster: 0,
        NumberOfAutoIndexesInCluster: 0,
        NumberOfCustomSortersInCluster: 0,
        NumberOfAnalyzersInCluster: 0,
        NumberOfSubscriptionsInCluster: 0,
    },
};

const licenseTiers: Record<LicenseType, number> = {
    None: 0,
    Invalid: 0,
    Reserved: 0,
    Community: 1,
    Essential: 1,
    Professional: 2,
    Enterprise: 3,
    EnterpriseAi: 4,
    Developer: 999, // Enterprise-level set of features
};

export const licenseSlice = createSlice({
    name: "license",
    initialState,
    reducers: {
        statusLoaded: (store, { payload: status }: PayloadAction<LicenseStatus>) => {
            store.status = status;

            // 0 in MaxClusterSize means Infinity, in other fields null means Infinity
            // for consistency we convert 0 to null
            if (
                store.status?.Attributes &&
                "MaxClusterSize" in store.status.Attributes &&
                store.status.Attributes.MaxClusterSize === 0
            ) {
                store.status.Attributes.MaxClusterSize = null;
                store.status.MaxClusterSize = null;
            }
        },
        supportLoaded: (store, { payload: status }: PayloadAction<Raven.Server.Commercial.LicenseSupportInfo>) => {
            store.support = status;
        },
        limitsUsageLoaded: (store, { payload: limitsUsage }: PayloadAction<LicenseLimitsUsage>) => {
            store.limitsUsage = limitsUsage;
        },
    },
});

export const licenseActions = licenseSlice.actions;

function statusValue<T extends keyof LicenseStatus>(key: T) {
    return (store: RootState) => store.license.status?.[key] ?? null;
}

const licenseRegistered = (store: RootState): boolean => {
    const licenseStatus = licenseSelectors.status(store);
    return !!licenseStatus && licenseStatus.Type !== "None" && licenseStatus.Type !== "Invalid";
};

const licenseInfo = (store: RootState) => {
    const type = licenseSelectors.licenseType(store) ?? "None";

    return {
        type,
        isAtLeast: (minimumType: LicenseType) => licenseTiers[type] >= licenseTiers[minimumType],
        isHigherThan: (compareType: LicenseType) => licenseTiers[type] > licenseTiers[compareType],
        isExact: (compareType: LicenseType) => licenseTiers[type] === licenseTiers[compareType],
        hasLicense: () => type !== "None" && type !== "Invalid",
    };
};

export const licenseSelectors = {
    status: (store: RootState) => store.license.status,
    licenseRegistered,
    statusValue,
    support: (store: RootState) => store.license.support,
    licenseType: (store: RootState) => store.license.status?.Type,
    limitsUsage: (store: RootState) => store.license.limitsUsage,
    licenseInfo,
};
