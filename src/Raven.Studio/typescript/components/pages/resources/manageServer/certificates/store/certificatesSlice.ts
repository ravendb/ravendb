import { createSlice } from "@reduxjs/toolkit";
import { loadStatus } from "components/models/common";

interface InitialState {
    certificates: TODO;
    certificatesLoadStatus: loadStatus;
    loadedServerCert: string;
    wellKnownAdminCerts: string[];
    wellKnownIssuers: string[];
}

const initialState: InitialState = {
    certificates: [],
    certificatesLoadStatus: "idle",
    loadedServerCert: null,
    wellKnownAdminCerts: [],
    wellKnownIssuers: [],
};

const certificatesSlice = createSlice({
    name: "certificates",
    initialState,
    reducers: {},
});

export const certificatesActions = {
    ...certificatesSlice.actions,
};
