import accessManager from "common/shell/accessManager";
import database from "models/resources/database";
import { DatabaseSharedInfo } from "components/models/databases";

function canHandleOperation(requiredAccess: accessLevel, dbName: string = null) {
    return accessManager.canHandleOperation(requiredAccess, dbName);
}

function canReadWriteDatabase(db: database) {
    return canHandleOperation("DatabaseReadWrite", db.name);
}

function canReadOnlyDatabase(db: database) {
    return accessManager.default.readOnlyOrAboveForDatabase(db);
}

function isOperatorOrAbove() {
    return accessManager.default.isOperatorOrAbove();
}

function isClusterAdminOrClusterNode() {
    return accessManager.default.isClusterAdminOrClusterNode();
}

function isSecuredServer() {
    return accessManager.default.secureServer();
}

function isAdminAccessOrAbove(db: database | DatabaseSharedInfo) {
    return accessManager.default.adminAccessOrAboveForDatabase(db);
}

export function useAccessManager() {
    return {
        canHandleOperation,
        canReadWriteDatabase,
        isSecuredServer,
        canReadOnlyDatabase,
        isOperatorOrAbove,
        isClusterAdminOrClusterNode,
        isAdminAccessOrAbove,
    };
}
