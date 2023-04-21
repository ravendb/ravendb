import { createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

interface DatabaseAccessInfo {
    databaseName: string;
    level: databaseAccessLevel;
}

interface AccessManagerState {
    databaseAccess: EntityState<DatabaseAccessInfo>;
    securityClearance: SecurityClearance;
}

const databaseAccessAdapter = createEntityAdapter<DatabaseAccessInfo>({
    selectId: (x) => x.databaseName,
});

const databaseAccessSelectors = databaseAccessAdapter.getSelectors();

const initialState: AccessManagerState = {
    databaseAccess: databaseAccessAdapter.getInitialState(),
    securityClearance: "ClusterAdmin",
};

const sliceName = "accessManager";

export const accessManagerSlice = createSlice({
    initialState,
    name: sliceName,
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
    },
});

export const { onDatabaseAccessLoaded, onSecurityClearanceSet } = accessManagerSlice.actions;

const selectDatabaseAccessLevel = (databaseName: string) => (store: RootState) =>
    databaseAccessSelectors.selectById(store.accessManager.databaseAccess, databaseName)?.level;

const selectOperatorOrAbove = (store: RootState) => {
    const clearance = store.accessManager.securityClearance;

    return clearance === "ClusterAdmin" || clearance === "ClusterNode" || clearance === "Operator";
};

const selectEffectiveDatabaseAccessLevel = (databaseName: string) => {
    const accessLevel = selectDatabaseAccessLevel(databaseName);

    return (store: RootState): databaseAccessLevel => {
        const operatorOrAbove = selectOperatorOrAbove(store);
        if (operatorOrAbove) {
            return "DatabaseAdmin";
        }

        return accessLevel(store);
    };
};

export const accessManagerSelectors = {
    databaseAccessLevel: selectDatabaseAccessLevel,
    operatorOrAbove: selectOperatorOrAbove,
    effectiveDatabaseAccessLevel: selectEffectiveDatabaseAccessLevel,
};
