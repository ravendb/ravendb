import { createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

interface DatabaseAccessInfo {
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

const databaseAccessSelectors = databaseAccessAdapter.getSelectors();

const initialState: AccessManagerState = {
    databaseAccess: databaseAccessAdapter.getInitialState(),
    securityClearance: "ClusterAdmin",
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

const selectDatabaseAccessLevel = (databaseName: string) => (store: RootState) =>
    databaseAccessSelectors.selectById(store.accessManager.databaseAccess, databaseName)?.level;

const selectIsOperatorOrAbove = (store: RootState) => {
    const clearance = store.accessManager.securityClearance;

    return clearance === "ClusterAdmin" || clearance === "ClusterNode" || clearance === "Operator";
};

const selectIsClusterAdminOrClusterNode = (store: RootState) => {
    const clearance = store.accessManager.securityClearance;

    return clearance === "ClusterAdmin" || clearance === "ClusterNode";
};

// If name is not provided, it will use the active database
const selectEffectiveDatabaseAccessLevel = (databaseName?: string) => {
    return (store: RootState): databaseAccessLevel => {
        const isOperatorOrAbove = selectIsOperatorOrAbove(store);
        if (isOperatorOrAbove) {
            return "DatabaseAdmin";
        }

        return selectDatabaseAccessLevel(databaseName ?? store.databases.activeDatabaseName)(store);
    };
};

const selectHasDatabaseAccessAdmin = (databaseName?: string) => {
    return (store: RootState) => {
        const effectiveDatabaseAccessLevel = selectEffectiveDatabaseAccessLevel(databaseName)(store);

        return effectiveDatabaseAccessLevel === "DatabaseAdmin";
    };
};

const selectHasDatabaseAccessWrite = (databaseName?: string) => {
    return (store: RootState) => {
        const effectiveDatabaseAccessLevel = selectEffectiveDatabaseAccessLevel(databaseName)(store);

        return effectiveDatabaseAccessLevel === "DatabaseAdmin" || effectiveDatabaseAccessLevel === "DatabaseReadWrite";
    };
};

const selectIsSecureServer = (store: RootState) => store.accessManager.isSecureServer;

export const accessManagerActions = accessManagerSlice.actions;

export const accessManagerSelectors = {
    isSecureServer: selectIsSecureServer,
    isOperatorOrAbove: selectIsOperatorOrAbove,
    isClusterAdminOrClusterNode: selectIsClusterAdminOrClusterNode,
    hasDatabaseAdminAccess: selectHasDatabaseAccessAdmin,
    hasDatabaseWriteAccess: selectHasDatabaseAccessWrite,
    effectiveDatabaseAccessLevel: selectEffectiveDatabaseAccessLevel,
};
