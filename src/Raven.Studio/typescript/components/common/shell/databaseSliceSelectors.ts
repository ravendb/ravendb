import { RootState } from "components/store";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databasesSliceInternal } from "components/common/shell/databasesSlice";
import { createSelector } from "@reduxjs/toolkit";

const selectActiveDatabase = (store: RootState) => store.databases.activeDatabase;

const { databasesSelectors } = databasesSliceInternal;

const selectAllDatabases = (store: RootState) => databasesSelectors.selectAll(store.databases.databases);

const selectAllDatabaseNames = createSelector(selectAllDatabases, (databases) => databases.map((x) => x.name));

const selectAllDatabasesCount = (store: RootState) => databasesSelectors.selectTotal(store.databases.databases);

function selectDatabaseByName(name: string) {
    return (store: RootState) => {
        if (DatabaseUtils.isSharded(name)) {
            const rootDatabaseName = DatabaseUtils.shardGroupKey(name);
            const rootDatabase = databasesSelectors.selectById(store.databases.databases, rootDatabaseName);

            if (!rootDatabase || !rootDatabase.sharded) {
                return null;
            }

            return rootDatabase.shards.find((x) => x.name === name);
        }
        return databasesSelectors.selectById(store.databases.databases, name);
    };
}

export const databaseSelectors = {
    activeDatabase: selectActiveDatabase,
    allDatabases: selectAllDatabases,
    allDatabaseNames: selectAllDatabaseNames,
    allDatabasesCount: selectAllDatabasesCount,
    databaseByName: selectDatabaseByName,
};
