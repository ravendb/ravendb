import nonShardedDatabase from "models/resources/nonShardedDatabase";
import shardedDatabase from "models/resources/shardedDatabase";
import DatabaseInfo = Raven.Client.ServerWide.Operations.DatabaseInfo;
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;

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
                Status: {
                    A: {
                        LastError: null,
                        LastStatus: "Ok",
                    },
                },
            },
        } as any;
    }

    static nonShardedSingleNodeDatabaseDto() {
        return DatabasesStubs.genericDatabaseInfo("db1");
    }

    static nonShardedSingleNodeDatabase() {
        const dto = DatabasesStubs.nonShardedSingleNodeDatabaseDto();
        const firstNodeTag = dto.NodesTopology.Members[0].NodeTag;
        return new nonShardedDatabase(dto, ko.observable(firstNodeTag));
    }

    static nonShardedClusterDatabaseDto() {
        const dbInfo = DatabasesStubs.genericDatabaseInfo("db1");
        dbInfo.NodesTopology.Members.push({
            NodeTag: "B",
            NodeUrl: "http://127.0.0.2:8080",
            ResponsibleNode: null,
        });
        dbInfo.NodesTopology.Status["B"] = {
            LastError: null,
            LastStatus: "Ok",
        };

        dbInfo.NodesTopology.Members.push({
            NodeTag: "C",
            NodeUrl: "http://127.0.0.3:8080",
            ResponsibleNode: null,
        });
        dbInfo.NodesTopology.Status["C"] = {
            LastError: null,
            LastStatus: "Ok",
        };

        return dbInfo;
    }

    static nonShardedClusterDatabase() {
        const dto = DatabasesStubs.nonShardedClusterDatabaseDto();
        const firstNodeTag = dto.NodesTopology.Members[0].NodeTag;
        return new nonShardedDatabase(dto, ko.observable(firstNodeTag));
    }

    static shardedDatabaseDto() {
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

        return [dbInfo1, dbInfo2, dbInfo3];
    }

    static shardedDatabase() {
        const shardedDtos = DatabasesStubs.shardedDatabaseDto();
        return new shardedDatabase(shardedDtos, ko.observable("A"));
    }

    static essentialStats(): EssentialDatabaseStatistics {
        return {
            CountOfTimeSeriesSegments: 5,
            CountOfTombstones: 3,
            CountOfAttachments: 10,
            CountOfDocumentsConflicts: 5,
            CountOfRevisionDocuments: 12,
            CountOfDocuments: 1_234_567,
            CountOfIndexes: 17,
            CountOfCounterEntries: 1_453,
            CountOfConflicts: 83,
            Indexes: [],
        };
    }

    static detailedStats(): DetailedDatabaseStatistics {
        const essential = DatabasesStubs.essentialStats();
        return {
            ...essential,
            CountOfIdentities: 17,
            CountOfCompareExchange: 38,
            DatabaseChangeVector:
                "A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, A:2568-OSKWIRBEDEGoAxbEIiFJeQ, A:8807-jMR/KF8hz0uMKFDXnmrQJA",
            CountOfTimeSeriesDeletedRanges: 9,
            Is64Bit: true,
            NumberOfTransactionMergerQueueOperations: 0,
            DatabaseId: "jMR/KF8hz0uMKFDXnmrQJA",
            CountOfCompareExchangeTombstones: 44,
            SizeOnDisk: {
                HumaneSize: "295.44 MBytes",
                SizeInBytes: 309788672,
            },
            TempBuffersSizeOnDisk: {
                HumaneSize: "17.19 MBytes",
                SizeInBytes: 18022400,
            },
            CountOfUniqueAttachments: essential.CountOfAttachments - 2,
            Pager: "Voron.Impl.Paging.RvnMemoryMapPager",
            StaleIndexes: [],
            Indexes: [],
        };
    }
}
