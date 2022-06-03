import accessManager from "common/shell/accessManager";
import database from "models/resources/database";

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

function isAdminAccessOrAbove(db: database) {
    return accessManager.default.adminAccessOrAboveForDatabase(db);
}

export function useAccessManager() {
    return {
        canHandleOperation,
        canReadWriteDatabase,
        canReadOnlyDatabase,
        isOperatorOrAbove,
        isClusterAdminOrClusterNode,
        isAdminAccessOrAbove,
    };
}
