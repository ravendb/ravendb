import { RootState } from "components/store";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databasesSliceInternal } from "components/common/shell/databasesSlice";

const selectActiveDatabaseName = (store: RootState) => store.databases.activeDatabaseName;

const { databasesSelectors } = databasesSliceInternal;

const selectAllDatabases = (store: RootState) => databasesSelectors.selectAll(store.databases.databases);
const selectAllDatabaseNames = (store: RootState) => databasesSelectors.selectIds(store.databases.databases);

const selectAllDatabasesCount = (store: RootState) => databasesSelectors.selectTotal(store.databases.databases);

function selectDatabaseByName(name: string) {
    return (store: RootState) => {
        if (DatabaseUtils.isSharded(name)) {
            const rootDatabaseName = DatabaseUtils.shardGroupKey(name);
            const rootDatabase = databasesSelectors.selectById(store.databases.databases, rootDatabaseName);

            if (!rootDatabase || !rootDatabase.isSharded) {
                return null;
            }

            return rootDatabase.shards.find((x) => x.name === name);
        }
        return databasesSelectors.selectById(store.databases.databases, name);
    };
}

function selectActiveDatabase(store: RootState) {
    const activeDatabaseName = selectActiveDatabaseName(store);
    if (!activeDatabaseName) {
        return null;
    }

    return selectDatabaseByName(activeDatabaseName)(store);
}

export const databaseSelectors = {
    activeDatabaseName: selectActiveDatabaseName,
    activeDatabase: selectActiveDatabase,
    allDatabases: selectAllDatabases,
    allDatabaseNames: selectAllDatabaseNames,
    allDatabasesCount: selectAllDatabasesCount,
    databaseByName: selectDatabaseByName,
};
