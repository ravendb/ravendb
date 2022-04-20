import nonShardedDatabase from "models/resources/nonShardedDatabase";
import shardedDatabase from "models/resources/shardedDatabase";
import DatabaseInfo = Raven.Client.ServerWide.Operations.DatabaseInfo;

export class DatabasesStubs {
    private static genericDatabaseInfo(name: string): DatabaseInfo {
        return {
            Name: name,
            IsEncrypted: false,
            LockMode: "Unlock",
            Alerts: 0,
            BackupInfo: null,
            Disabled: false,
            DynamicNodesDistribution: false,
            HasExpirationConfiguration: false,
            HasRefreshConfiguration: false,
            HasRevisionsConfiguration: false,
            DeletionInProgress: null,
            IsAdmin: true,
            Environment: "None",
            LoadError: null,
            RejectClients: false,
            NodesTopology: {
                Members: [
                    {
                        NodeTag: "A",
                        NodeUrl: "http://127.0.0.1:8080",
                        ResponsibleNode: null,
                    },
                ],
                Promotables: [],
                Rehabs: [],
                PriorityOrder: null,
                Status: null,
            },
        } as any;
    }

    static nonShardedSingleNodeDatabase() {
        return new nonShardedDatabase(DatabasesStubs.genericDatabaseInfo("db1"), ko.observable("A"));
    }

    static nonShardedClusterDatabase() {
        const dbInfo = DatabasesStubs.genericDatabaseInfo("db1");
        dbInfo.NodesTopology.Members.push({
            NodeTag: "B",
            NodeUrl: "http://127.0.0.2:8080",
            ResponsibleNode: null,
        });
        dbInfo.NodesTopology.Members.push({
            NodeTag: "C",
            NodeUrl: "http://127.0.0.3:8080",
            ResponsibleNode: null,
        });
        return new nonShardedDatabase(dbInfo, ko.observable("A"));
    }

    static shardedDatabase() {
        const dbInfo1 = DatabasesStubs.genericDatabaseInfo("sharded$0");
        dbInfo1.NodesTopology.Members = [
            {
                NodeTag: "A",
                NodeUrl: "http://127.0.0.1:8080",
                ResponsibleNode: null,
            },
            {
                NodeTag: "B",
                NodeUrl: "http://127.0.0.2:8080",
                ResponsibleNode: null,
            },
        ];
        const dbInfo2 = DatabasesStubs.genericDatabaseInfo("sharded$1");
        dbInfo2.NodesTopology.Members = [
            {
                NodeTag: "C",
                NodeUrl: "http://127.0.0.3:8080",
                ResponsibleNode: null,
            },
            {
                NodeTag: "D",
                NodeUrl: "http://127.0.0.4:8080",
                ResponsibleNode: null,
            },
        ];
        const dbInfo3 = DatabasesStubs.genericDatabaseInfo("sharded$2");
        dbInfo3.NodesTopology.Members = [
            {
                NodeTag: "E",
                NodeUrl: "http://127.0.0.5:8080",
                ResponsibleNode: null,
            },
            {
                NodeTag: "F",
                NodeUrl: "http://127.0.0.6:8080",
                ResponsibleNode: null,
            },
        ];

        return new shardedDatabase([dbInfo1, dbInfo2, dbInfo3], ko.observable("A"));
    }
}
