import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import LicenseStatus = Raven.Server.Commercial.LicenseStatus;

interface LicenseState {
    status: LicenseStatus;
}

const initialState: LicenseState = {
    status: null,
};

export const licenseSlice = createSlice({
    name: "license",
    initialState,
    reducers: {
        statusLoaded: (store, { payload: status }: PayloadAction<LicenseStatus>) => {
            store.status = status;
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
};
