import { RootState } from "components/store";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { ShardedDatabaseSharedInfo } from "components/models/databases";
import { databasesSliceInternal } from "components/common/shell/databasesSlice";

const selectActiveDatabase = (store: RootState) => store.databases.activeDatabase;

const { databasesSelectors } = databasesSliceInternal;

const selectAllDatabases = (store: RootState) => databasesSelectors.selectAll(store.databases.databases);
const selectAllDatabaseNames = (store: RootState) =>
    databasesSelectors.selectIds(store.databases.databases) as string[];

const selectAllDatabasesCount = (store: RootState) => databasesSelectors.selectTotal(store.databases.databases);

function selectDatabaseByName(name: string) {
    return (store: RootState) => {
        if (DatabaseUtils.isSharded(name)) {
            const rootDatabaseName = DatabaseUtils.shardGroupKey(name);
            const rootDatabase = databasesSelectors.selectById(
                store.databases.databases,
                rootDatabaseName
            ) as ShardedDatabaseSharedInfo;
            if (!rootDatabase) {
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
