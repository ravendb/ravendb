import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import LicenseStatus = Raven.Server.Commercial.LicenseStatus;
import LicenseLimitsUsage = Raven.Server.Commercial.LicenseLimitsUsage;

interface LicenseState {
    status: LicenseStatus;
    limitsUsage: LicenseLimitsUsage;
}

const initialState: LicenseState = {
    status: null,
    limitsUsage: {
        ClusterAutoIndexes: null,
        ClusterStaticIndexes: null,
        ClusterSubscriptionTasks: null,
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
    return (store: RootState) => store.license.status[key] ?? null;
}

export const licenseSelectors = {
    statusValue,
    licenseType: (store: RootState) => store.license.status?.Type,
    limitsUsage: (store: RootState) => store.license.limitsUsage,
};
