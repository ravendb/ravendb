import { RootState } from "components/store";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databasesSliceInternal } from "components/common/shell/databasesSlice";

const selectActiveDatabaseName = (store: RootState) => store.databases.activeDatabaseName;

const { databasesSelectors } = databasesSliceInternal;

const selectAllDatabases = (store: RootState) => databasesSelectors.selectAll(store.databases.databases);

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
    activeDatabaseName: selectActiveDatabaseName,
    allDatabases: selectAllDatabases,
    allDatabasesCount: selectAllDatabasesCount,
    databaseByName: selectDatabaseByName,
};
