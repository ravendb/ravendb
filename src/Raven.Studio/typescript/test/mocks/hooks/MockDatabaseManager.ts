import { DatabaseSharedInfo } from "components/models/databases";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { MockedValue } from "test/mocks/services/AutoMockService";
import { createValue } from "../utils";

type ManagerState = {
    databasesLocal: DatabaseSharedInfo[];
};

const mockDatabaseManagerState = ko.observable<ManagerState>({
    databasesLocal: [],
});

export class MockDatabaseManager {
    get state() {
        return mockDatabaseManagerState;
    }

    with_Cluster(dto?: MockedValue<DatabaseSharedInfo>) {
        const value = this.createValue(dto, DatabasesStubs.nonShardedClusterDatabase().toDto());
        this.updateState({
            databasesLocal: [value],
        });
    }

    with_Sharded(dto?: MockedValue<DatabaseSharedInfo>) {
        const value = this.createValue(dto, DatabasesStubs.shardedDatabase().toDto());
        this.updateState({
            databasesLocal: [value],
        });
    }

    with_Single(dto?: MockedValue<DatabaseSharedInfo>) {
        const value = this.createValue(dto, DatabasesStubs.nonShardedSingleNodeDatabase().toDto());
        this.updateState({
            databasesLocal: [value],
        });
    }

    withDatabases(dbs: DatabaseSharedInfo[]) {
        this.updateState({
            databasesLocal: dbs,
        });
    }

    private updateState(update: Partial<ManagerState>) {
        const oldState = mockDatabaseManagerState();
        mockDatabaseManagerState({
            ...oldState,
            ...update,
        });
    }

    databases() {
        return mockDatabaseManagerState().databasesLocal;
    }

    protected createValue<T>(value: MockedValue<T>, defaultValue: T): T {
        return createValue(value, defaultValue);
    }
}
