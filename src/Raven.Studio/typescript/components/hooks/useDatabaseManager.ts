import databasesManager from "common/shell/databasesManager";
import { useCallback, useEffect, useState } from "react";
import { DatabaseSharedInfo } from "components/models/databases";

export function useDatabaseManager() {
    const [databases, setDatabases] = useState<DatabaseSharedInfo[]>([]);

    const onUpdate = useCallback(() => {
        const newDatabases = databasesManager.default.databases().map((x) => x.toDto());
        setDatabases(newDatabases);
    }, []);

    const findByName = useCallback((name: string) => {
        const db = databasesManager.default.getDatabaseByName(name);
        return db?.toDto();
    }, []);

    useEffect(() => {
        databasesManager.default.onUpdateStatsCallbacks.push(onUpdate);
        onUpdate();

        return () => {
            databasesManager.default.onUpdateStatsCallbacks.remove(onUpdate);
        };
    }, [onUpdate]);

    return {
        databases,
        findByName,
    };
}
