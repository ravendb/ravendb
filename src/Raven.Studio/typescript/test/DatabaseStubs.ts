export class DatabaseStubs {
    static shardedDatabasesResponse() {
        const shard0 = {
            Name: "db$0",
        } as Raven.Client.ServerWide.Operations.DatabaseInfo;

        const shard1 = {
            Name: "db$1",
        } as Raven.Client.ServerWide.Operations.DatabaseInfo;

        const fakeResponse: Raven.Client.ServerWide.Operations.DatabasesInfo = {
            Databases: [shard0, shard1],
        };

        return fakeResponse;
    }

    static singleDatabaseResponse() {
        const db1 = {
            Name: "db1",
        } as Raven.Client.ServerWide.Operations.DatabaseInfo;

        const fakeResponse: Raven.Client.ServerWide.Operations.DatabasesInfo = {
            Databases: [db1],
        };

        return fakeResponse;
    }
}
