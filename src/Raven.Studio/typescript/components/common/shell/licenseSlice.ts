import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import LicenseLimitsUsage = Raven.Server.Commercial.LicenseLimitsUsage;

interface LicenseState {
    status: LicenseStatus;
    limitsUsage: LicenseLimitsUsage;
}

const initialState: LicenseState = {
    status: null,
    limitsUsage: {
        NumberOfStaticIndexesInCluster: 0,
        NumberOfAutoIndexesInCluster: 0,
        NumberOfCustomSortersInCluster: 0,
        NumberOfAnalyzersInCluster: 0,
        NumberOfSubscriptionsInCluster: 0,
    },
};

export const licenseSlice = createSlice({
    name: "license",
    initialState,
    reducers: {
        statusLoaded: (store, { payload: status }: PayloadAction<LicenseStatus>) => {
            store.status = status;
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

const isEnterpriseOrDeveloper = (store: RootState): boolean => {
    const type = licenseSelectors.licenseType(store);

    return type === "Enterprise" || type === "Developer";
};

const isProfessionalOrAbove = (store: RootState): boolean => {
    return isEnterpriseOrDeveloper(store) || licenseSelectors.licenseType(store) === "Professional";
};

export const licenseSelectors = {
    statusValue,
    licenseType: (store: RootState) => store.license.status?.Type,
    limitsUsage: (store: RootState) => store.license.limitsUsage,
    isEnterpriseOrDeveloper,
    isProfessionalOrAbove,
};
