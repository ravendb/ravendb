import { DatabaseSharedInfo } from "components/models/databases";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { MockedValue } from "test/mocks/services/AutoMockService";
import { createValue } from "../utils";
import { globalDispatch } from "components/storeCompat";
import { databaseActions } from "components/common/shell/databaseSliceActions";

export class MockDatabaseManager {
    with_Cluster(dto?: MockedValue<DatabaseSharedInfo>) {
        const value = this.createValue(dto, DatabasesStubs.nonShardedClusterDatabase().toDto());

        globalDispatch(databaseActions.databasesLoaded([value]));
        return value;
    }

    with_Sharded(dto?: MockedValue<DatabaseSharedInfo>) {
        const value = this.createValue(dto, DatabasesStubs.shardedDatabase().toDto());
        globalDispatch(databaseActions.databasesLoaded([value]));
        return value;
    }

    with_Single(dto?: MockedValue<DatabaseSharedInfo>) {
        const value = this.createValue(dto, DatabasesStubs.nonShardedSingleNodeDatabase().toDto());
        globalDispatch(databaseActions.databasesLoaded([value]));
        return value;
    }

    withDatabases(dbs: DatabaseSharedInfo[]) {
        globalDispatch(databaseActions.databasesLoaded(dbs));
    }

    protected createValue<T>(value: MockedValue<T>, defaultValue: T): T {
        return createValue(value, defaultValue);
    }
}
