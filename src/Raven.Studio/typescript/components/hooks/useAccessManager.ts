import accessManager from "common/shell/accessManager";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";

export function useAccessManager() {
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    function canHandleOperation(requiredAccess: accessLevel, databaseName: string = activeDatabaseName) {
        return accessManager.canHandleOperation(requiredAccess, databaseName);
    }

    function canReadWriteDatabase(databaseName: string = activeDatabaseName) {
        return canHandleOperation("DatabaseReadWrite", databaseName);
    }

    function canReadOnlyDatabase(databaseName: string = activeDatabaseName) {
        return accessManager.default.readOnlyOrAboveForDatabase(databaseName);
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

    function isAdminAccessOrAbove(databaseName: string = activeDatabaseName) {
        return accessManager.default.adminAccessOrAboveForDatabase(databaseName);
    }

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
