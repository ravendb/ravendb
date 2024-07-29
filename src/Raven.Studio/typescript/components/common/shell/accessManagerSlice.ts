import { createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export interface DatabaseAccessInfo {
    databaseName: string;
    level: databaseAccessLevel;
}

interface AccessManagerState {
    databaseAccess: EntityState<DatabaseAccessInfo, string>;
    securityClearance: SecurityClearance;
    isSecureServer: boolean;
}

const databaseAccessAdapter = createEntityAdapter<DatabaseAccessInfo, string>({
    selectId: (x) => x.databaseName,
});

export const databaseAccessSelectors = databaseAccessAdapter.getSelectors();

const initialState: AccessManagerState = {
    databaseAccess: databaseAccessAdapter.getInitialState(),
    securityClearance: null,
    isSecureServer: false,
};

export const accessManagerSlice = createSlice({
    initialState,
    name: "accessManager",
    reducers: {
        onDatabaseAccessLoaded: (state, action: PayloadAction<dictionary<databaseAccessLevel>>) => {
            const items: DatabaseAccessInfo[] = Object.entries(action.payload).map((value) => {
                return {
                    level: value[1],
                    databaseName: value[0],
                };
            });
            databaseAccessAdapter.setAll(state.databaseAccess, items);
        },
        onSecurityClearanceSet: (state, action: PayloadAction<SecurityClearance>) => {
            state.securityClearance = action.payload;
        },
        onIsSecureServerSet: (state, action: PayloadAction<boolean>) => {
            state.isSecureServer = action.payload;
        },
    },
});

export const accessManagerActions = accessManagerSlice.actions;
