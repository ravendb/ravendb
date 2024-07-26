import { RootState } from "components/store";
import SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
import { createSelector, EntityState } from "@reduxjs/toolkit";
import { DatabaseAccessInfo, databaseAccessSelectors } from "components/common/shell/accessManagerSlice";

// If database name is not provided, it will use the active one
interface GetAccessLevelArgs {
    databaseAccess: EntityState<DatabaseAccessInfo, string>;
    activeDatabaseName: string;
    databaseName?: string;
}

interface GetEffectiveAccessLevelArgs extends GetAccessLevelArgs {
    securityClearance: SecurityClearance;
}

// Getters

function getDatabaseAccessLevel(args: GetAccessLevelArgs) {
    return databaseAccessSelectors.selectById(args.databaseAccess, args.databaseName ?? args.activeDatabaseName)?.level;
}

function getIsOperatorOrAbove(securityClearance: SecurityClearance) {
    return (
        securityClearance === "ClusterAdmin" || securityClearance === "ClusterNode" || securityClearance === "Operator"
    );
}

function getIsClusterAdminOrClusterNode(securityClearance: SecurityClearance) {
    return securityClearance === "ClusterAdmin" || securityClearance === "ClusterNode";
}

const getEffectiveDatabaseAccessLevel = (args: GetEffectiveAccessLevelArgs) => {
    const isOperatorOrAbove = getIsOperatorOrAbove(args.securityClearance);
    if (isOperatorOrAbove) {
        return "DatabaseAdmin";
    }

    return getDatabaseAccessLevel(args);
};

const getHasDatabaseAccessAdmin = (args: GetEffectiveAccessLevelArgs) => {
    const effectiveDatabaseAccessLevel = getEffectiveDatabaseAccessLevel(args);
    return effectiveDatabaseAccessLevel === "DatabaseAdmin";
};

const getHasDatabaseAccessWrite = (args: GetEffectiveAccessLevelArgs) => {
    const effectiveDatabaseAccessLevel = getEffectiveDatabaseAccessLevel(args);
    return effectiveDatabaseAccessLevel === "DatabaseAdmin" || effectiveDatabaseAccessLevel === "DatabaseReadWrite";
};

// Selectors

const selectIsOperatorOrAbove = createSelector(
    (store: RootState) => store.accessManager.securityClearance,
    (securityClearance: SecurityClearance) => getIsOperatorOrAbove(securityClearance)
);

const selectIsClusterAdminOrClusterNode = createSelector(
    (store: RootState) => store.accessManager.securityClearance,
    (securityClearance: SecurityClearance) => getIsClusterAdminOrClusterNode(securityClearance)
);

const selectGetEffectiveDatabaseAccessLevel = createSelector(
    [
        (store: RootState) => store.accessManager.securityClearance,
        (store: RootState) => store.accessManager.databaseAccess,
        (store: RootState) => store.databases.activeDatabaseName,
    ],
    (
        securityClearance: SecurityClearance,
        databaseAccess: EntityState<DatabaseAccessInfo, string>,
        activeDatabaseName: string
    ) =>
        (databaseName?: string) => {
            return getEffectiveDatabaseAccessLevel({
                securityClearance,
                databaseAccess,
                activeDatabaseName,
                databaseName,
            });
        }
);

const selectGetHasDatabaseAccessAdmin = createSelector(
    [
        (store: RootState) => store.accessManager.securityClearance,
        (store: RootState) => store.accessManager.databaseAccess,
        (store: RootState) => store.databases.activeDatabaseName,
    ],
    (
        securityClearance: SecurityClearance,
        databaseAccess: EntityState<DatabaseAccessInfo, string>,
        activeDatabaseName: string
    ) =>
        (databaseName?: string) => {
            return getHasDatabaseAccessAdmin({ securityClearance, databaseAccess, activeDatabaseName, databaseName });
        }
);

const selectGetHasDatabaseAccessWrite = createSelector(
    [
        (store: RootState) => store.accessManager.securityClearance,
        (store: RootState) => store.accessManager.databaseAccess,
        (store: RootState) => store.databases.activeDatabaseName,
    ],
    (
        securityClearance: SecurityClearance,
        databaseAccess: EntityState<DatabaseAccessInfo, string>,
        activeDatabaseName: string
    ) =>
        (databaseName?: string) => {
            return getHasDatabaseAccessWrite({ securityClearance, databaseAccess, activeDatabaseName, databaseName });
        }
);

const selectGetCanHandleOperation = createSelector(
    [
        (store: RootState) => store.accessManager.securityClearance,
        (store: RootState) => store.accessManager.databaseAccess,
        (store: RootState) => store.databases.activeDatabaseName,
    ],
    (
        securityClearance: SecurityClearance,
        databaseAccess: EntityState<DatabaseAccessInfo, string>,
        activeDatabaseName: string
    ) =>
        (requiredAccess: accessLevel, databaseName: string = null) => {
            const actualAccessLevel = getIsOperatorOrAbove(securityClearance)
                ? securityClearance
                : getDatabaseAccessLevel({ databaseAccess, activeDatabaseName, databaseName });

            if (!actualAccessLevel) {
                return false;
            }

            const clusterAdminOrNode = actualAccessLevel === "ClusterAdmin" || actualAccessLevel === "ClusterNode";
            const operator = actualAccessLevel === "Operator";
            const dbAdmin = actualAccessLevel === "DatabaseAdmin";
            const dbReadWrite = actualAccessLevel === "DatabaseReadWrite";
            const dbRead = actualAccessLevel === "DatabaseRead";

            switch (requiredAccess) {
                case "ClusterAdmin":
                case "ClusterNode":
                    return clusterAdminOrNode;
                case "Operator":
                    return clusterAdminOrNode || operator;
                case "DatabaseAdmin":
                    return clusterAdminOrNode || operator || dbAdmin;
                case "DatabaseReadWrite":
                    return clusterAdminOrNode || operator || dbAdmin || dbReadWrite;
                case "DatabaseRead":
                    return clusterAdminOrNode || operator || dbAdmin || dbReadWrite || dbRead;
                default:
                    return false;
            }
        }
);

const selectIsSecureServer = (store: RootState) => store.accessManager.isSecureServer;

export const accessManagerSelectors = {
    isSecureServer: selectIsSecureServer,
    isOperatorOrAbove: selectIsOperatorOrAbove,
    isClusterAdminOrClusterNode: selectIsClusterAdminOrClusterNode,
    getHasDatabaseAdminAccess: selectGetHasDatabaseAccessAdmin,
    getHasDatabaseWriteAccess: selectGetHasDatabaseAccessWrite,
    getEffectiveDatabaseAccessLevel: selectGetEffectiveDatabaseAccessLevel,
    getCanHandleOperation: selectGetCanHandleOperation,
};
