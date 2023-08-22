import nonShardedDatabase from "models/resources/nonShardedDatabase";
import shardedDatabase from "models/resources/shardedDatabase";
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import StudioDatabaseInfo = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabases.StudioDatabaseInfo;
import DatabaseGroupNodeStatus = Raven.Client.ServerWide.Operations.DatabaseGroupNodeStatus;
import StudioDatabaseState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseState;
import RefreshConfiguration = Raven.Client.Documents.Operations.Refresh.RefreshConfiguration;
import ExpirationConfiguration = Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration;
import RevisionsConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration;
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;

export class DatabasesStubs {
    private static genericDatabaseInfo(name: string): StudioDatabaseInfo {
        return {
            Name: name,
            IsEncrypted: false,
            LockMode: "Unlock",
            DeletionInProgress: {},
            Sharding: null,
            IndexesCount: 0,
            NodesTopology: {
                PriorityOrder: [],
                Members: [
                    {
                        NodeTag: "A",
                        ResponsibleNode: null,
                        NodeUrl: "http://a.ravendb",
                    },
                ],
                Promotables: [],
                Rehabs: [],
                Status: {
                    A: DatabasesStubs.statusOk(),
                },
                DynamicNodesDistribution: false,
            },
            IsDisabled: false,
            HasRefreshConfiguration: false,
            HasExpirationConfiguration: false,
            HasRevisionsConfiguration: false,
            StudioEnvironment: "None",
            IsSharded: false,
        };
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
            NodeUrl: "http://b.ravendb",
            ResponsibleNode: null,
        });
        dbInfo.NodesTopology.PriorityOrder = [];
        dbInfo.NodesTopology.Members.push({
            NodeTag: "C",
            NodeUrl: "http://c.ravendb",
            ResponsibleNode: null,
        });
        dbInfo.NodesTopology.Status["B"] = DatabasesStubs.statusOk();
        dbInfo.NodesTopology.Status["C"] = DatabasesStubs.statusOk();
        return dbInfo;
    }

    static nonShardedClusterDatabase() {
        const dto = DatabasesStubs.nonShardedClusterDatabaseDto();
        const firstNodeTag = dto.NodesTopology.Members[0].NodeTag;
        return new nonShardedDatabase(dto, ko.observable(firstNodeTag));
    }

    static shardedDatabaseDto(): StudioDatabaseInfo {
        const dbInfo = DatabasesStubs.genericDatabaseInfo("sharded");
        dbInfo.NodesTopology = null;
        dbInfo.IsSharded = true;
        dbInfo.IndexesCount = 5;
        dbInfo.Sharding = {
            Shards: {
                [0]: {
                    Members: [
                        {
                            NodeTag: "A",
                            NodeUrl: "http://a.ravendb",
                            ResponsibleNode: null,
                        },
                        {
                            NodeTag: "B",
                            NodeUrl: "http://b.ravendb",
                            ResponsibleNode: null,
                        },
                    ],
                    Rehabs: [],
                    Promotables: [],
                    PriorityOrder: [],
                    Status: {
                        A: DatabasesStubs.statusOk(),
                        B: DatabasesStubs.statusOk(),
                    },
                },
                [1]: {
                    Members: [
                        {
                            NodeTag: "B",
                            NodeUrl: "http://b.ravendb",
                            ResponsibleNode: null,
                        },
                        {
                            NodeTag: "C",
                            NodeUrl: "http://c.ravendb",
                            ResponsibleNode: null,
                        },
                    ],
                    Rehabs: [],
                    Promotables: [],
                    PriorityOrder: [],
                    Status: {
                        B: DatabasesStubs.statusOk(),
                        C: DatabasesStubs.statusOk(),
                    },
                },
                [2]: {
                    Members: [
                        {
                            NodeTag: "A",
                            NodeUrl: "http://a.ravendb",
                            ResponsibleNode: null,
                        },
                        {
                            NodeTag: "C",
                            NodeUrl: "http://c.ravendb",
                            ResponsibleNode: null,
                        },
                    ],
                    Rehabs: [],
                    Promotables: [],
                    PriorityOrder: [],
                    Status: {
                        A: DatabasesStubs.statusOk(),
                        C: DatabasesStubs.statusOk(),
                    },
                },
            },
            Orchestrator: {
                NodesTopology: {
                    Members: [
                        {
                            NodeTag: "A",
                            NodeUrl: "http://a.ravendb",
                            ResponsibleNode: null,
                        },
                        {
                            NodeTag: "B",
                            NodeUrl: "http://b.ravendb",
                            ResponsibleNode: null,
                        },
                        {
                            NodeTag: "C",
                            NodeUrl: "http://c.ravendb",
                            ResponsibleNode: null,
                        },
                    ],
                    Promotables: [],
                    Rehabs: [],
                    PriorityOrder: [],
                    Status: {
                        A: DatabasesStubs.statusOk(),
                        B: DatabasesStubs.statusOk(),
                        C: DatabasesStubs.statusOk(),
                    },
                },
            },
        } as any;

        return dbInfo;
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

    static studioDatabaseState(dbName: string): StudioDatabaseState {
        return {
            Name: dbName,
            UpTime: "00:05:00",
            IndexingStatus: "Running",
            LoadError: null,
            BackupInfo: null,
            DocumentsCount: 1024,
            Alerts: 1,
            PerformanceHints: 2,
            IndexingErrors: 3,
            TotalSize: {
                SizeInBytes: 5,
                HumaneSize: "5 Bytes",
            },
            TempBuffersSize: {
                SizeInBytes: 2,
                HumaneSize: "2 Bytes",
            },
            DatabaseStatus: "Online",
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

    private static statusOk(): DatabaseGroupNodeStatus {
        return {
            LastStatus: "Ok",
            LastError: null,
        };
    }

    static refreshConfiguration(): RefreshConfiguration {
        return {
            Disabled: false,
            RefreshFrequencyInSec: 65,
        };
    }

    static expirationConfiguration(): ExpirationConfiguration {
        return {
            Disabled: false,
            DeleteFrequencyInSec: 65,
        };
    }

    static tombstonesState(): TombstonesStateOnWire {
        return {
            MinAllDocsEtag: 9223372036854776000,
            MinAllTimeSeriesEtag: 9223372036854776000,
            MinAllCountersEtag: 9223372036854776000,
            Results: [
                {
                    Collection: "Attachments.Tombstones",
                    Documents: {
                        Component: null,
                        Etag: 9223372036854776000,
                    },
                    TimeSeries: {
                        Component: null,
                        Etag: 9223372036854776000,
                    },
                    Counters: {
                        Component: null,
                        Etag: 9223372036854776000,
                    },
                },
                {
                    Collection: "Revisions.Tombstones",
                    Documents: {
                        Component: null,
                        Etag: 9223372036854776000,
                    },
                    TimeSeries: {
                        Component: null,
                        Etag: 9223372036854776000,
                    },
                    Counters: {
                        Component: null,
                        Etag: 9223372036854776000,
                    },
                },
                {
                    Collection: "docs",
                    Documents: {
                        Component: "Index 'test'",
                        Etag: 0,
                    },
                    TimeSeries: {
                        Component: null,
                        Etag: 9223372036854776000,
                    },
                    Counters: {
                        Component: null,
                        Etag: 9223372036854776000,
                    },
                },
            ],
            PerSubscriptionInfo: [
                {
                    Identifier: "Index 'test'",
                    Type: "Documents",
                    Collection: "Docs",
                    Etag: 0,
                },
            ],
        };
    }

    static revisionsConfiguration(): RevisionsConfiguration {
        return {
            Default: {
                Disabled: false,
                MinimumRevisionsToKeep: 9,
                MinimumRevisionAgeToKeep: "55.20:14:44",
                PurgeOnDelete: true,
                MaximumRevisionsToDeleteUponDocumentUpdate: 120,
            },
            Collections: {
                Categories: {
                    Disabled: true,
                    MinimumRevisionsToKeep: 16,
                    MinimumRevisionAgeToKeep: "65.00:00:00",
                    PurgeOnDelete: true,
                    MaximumRevisionsToDeleteUponDocumentUpdate: 80,
                },
                Shippers: {
                    Disabled: false,
                    MinimumRevisionsToKeep: null,
                    MinimumRevisionAgeToKeep: null,
                    PurgeOnDelete: false,
                    MaximumRevisionsToDeleteUponDocumentUpdate: null,
                },
            },
        };
    }

    static revisionsForConflictsConfiguration(): RevisionsCollectionConfiguration {
        return {
            Disabled: true,
            MinimumRevisionsToKeep: null,
            MinimumRevisionAgeToKeep: "55.00:00:00",
            PurgeOnDelete: false,
            MaximumRevisionsToDeleteUponDocumentUpdate: 100,
        };
    }
}
