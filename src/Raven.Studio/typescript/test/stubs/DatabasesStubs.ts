import nonShardedDatabase from "models/resources/nonShardedDatabase";
import shardedDatabase from "models/resources/shardedDatabase";
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import StudioDatabaseInfo = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabases.StudioDatabaseInfo;
import DatabaseGroupNodeStatus = Raven.Client.ServerWide.Operations.DatabaseGroupNodeStatus;
import StudioDatabaseState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseState;
import RefreshConfiguration = Raven.Client.Documents.Operations.Refresh.RefreshConfiguration;
import DataArchival = Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration;
import ExpirationConfiguration = Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration;
import RevisionsConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration;
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;
import SorterDefinition = Raven.Client.Documents.Queries.Sorting.SorterDefinition;
import AnalyzerDefinition = Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition;
import document from "models/database/documents/document";

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
            HasDataArchivalConfiguration: false,
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
            RefreshFrequencyInSec: 129599,
        };
    }

    static expirationConfiguration(): ExpirationConfiguration {
        return {
            Disabled: false,
            DeleteFrequencyInSec: 129599,
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
            Default: null,
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

    static customAnalyzers(): AnalyzerDefinition[] {
        return [{ Code: "database-analyzer-code-1", Name: "First Database analyzer" }];
    }

    static customSorters(): SorterDefinition[] {
        return [{ Code: "database-sorter-code-1", Name: "First Database sorter" }];
    }

    static dataArchivalConfiguration(): DataArchival {
        return {
            Disabled: false,
            ArchiveFrequencyInSec: 65,
        };
    }

    static documentsCompressionConfiguration(): Raven.Client.ServerWide.DocumentsCompressionConfiguration {
        return {
            Collections: ["Shippers"],
            CompressAllCollections: false,
            CompressRevisions: true,
        };
    }
    static emptyConnectionStrings(): Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult {
        return {
            ElasticSearchConnectionStrings: {},
            OlapConnectionStrings: {},
            QueueConnectionStrings: {},
            RavenConnectionStrings: {},
            SqlConnectionStrings: {},
        };
    }

    static connectionStrings(): Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult {
        return {
            RavenConnectionStrings: {
                "raven-name (used by task)": {
                    Type: "Raven",
                    Name: "raven-name (used by task)",
                    Database: "some-db",
                    TopologyDiscoveryUrls: ["http://test"],
                },
            },
            SqlConnectionStrings: {
                "sql-name": {
                    Type: "Sql",
                    Name: "sql-name",
                    ConnectionString: "some-connection-string",
                    FactoryName: "System.Data.SqlClient",
                },
            },
            OlapConnectionStrings: {
                "olap-name": {
                    Type: "Olap",
                    Name: "olap-name",
                    LocalSettings: {
                        Disabled: false,
                        GetBackupConfigurationScript: null,
                        FolderPath: "/bin",
                    },
                    S3Settings: null,
                    AzureSettings: null,
                    GlacierSettings: null,
                    GoogleCloudSettings: null,
                    FtpSettings: null,
                },
            },
            ElasticSearchConnectionStrings: {
                "elasticsearch-name": {
                    Type: "ElasticSearch",
                    Name: "elasticsearch-name",
                    Nodes: ["http://test"],
                    EnableCompatibilityMode: false,
                    Authentication: {
                        Basic: null,
                        ApiKey: null,
                        Certificate: {
                            CertificatesBase64: [
                                "Certificate:    Data:        Version: 3 (0x2)        Serial Number: 1 (0x1)        Signature Algorithm: sha1WithRSAEncryption        Issuer: C=FR, ST=Alsace, L=Strasbourg, O=www.freelan.org, OU=freelan, CN=Freelan Sample Certificate Authority/emailAddress=contact@freelan.org        Validity            Not Before: Apr 27 10:31:18 2012 GMT            Not After : Apr 25 10:31:18 2022 GMT        Subject: C=FR, ST=Alsace, O=www.freelan.org, OU=freelan, CN=alice/emailAddress=contact@freelan.org        Subject Public Key Info:            Public Key Algorithm: rsaEncryption                Public-Key: (4096 bit)                Modulus:                    00:dd:6d:bd:f8:80:fa:d7:de:1b:1f:a7:a3:2e:b2:                    02:e2:16:f6:52:0a:3c:bf:a6:42:f8:ca:dc:93:67:                    4d:60:c3:4f:8d:c3:8a:00:1b:f1:c4:4b:41:6a:69:                    d2:69:e5:3f:21:8e:c5:0b:f8:22:37:ad:b6:2c:4b:                    55:ff:7a:03:72:bb:9a:d3:ec:96:b9:56:9f:cb:19:                    99:c9:32:94:6f:8f:c6:52:06:9f:45:03:df:fd:e8:                    97:f6:ea:d6:ba:bb:48:2b:b5:e0:34:61:4d:52:36:                    0f:ab:87:52:25:03:cf:87:00:87:13:f2:ca:03:29:                    16:9d:90:57:46:b5:f4:0e:ae:17:c8:0a:4d:92:ed:                    08:a6:32:23:11:71:fe:f2:2c:44:d7:6c:07:f3:0b:                    7b:0c:4b:dd:3b:b4:f7:37:70:9f:51:b6:88:4e:5d:                    6a:05:7f:8d:9b:66:7a:ab:80:20:fe:ee:6b:97:c3:                    49:7d:78:3b:d5:99:97:03:75:ce:8f:bc:c5:be:9c:                    9a:a5:12:19:70:f9:a4:bd:96:27:ed:23:02:a7:c7:                    57:c9:71:cf:76:94:a2:21:62:f6:b8:1d:ca:88:ee:                    09:ad:46:2f:b7:61:b3:2c:15:13:86:9f:a5:35:26:                    5a:67:f4:37:c8:e6:80:01:49:0e:c7:ed:61:d3:cd:                    bc:e4:f8:be:3f:c9:4e:f8:7d:97:89:ce:12:bc:ca:                    b5:c6:d2:e0:d9:b3:68:3c:2e:4a:9d:b4:5f:b8:53:                    ee:50:3d:bf:dd:d4:a2:8a:b6:a0:27:ab:98:0c:b3:                    b2:58:90:e2:bc:a1:ad:ff:bd:8e:55:31:0f:00:bf:                    68:e9:3d:a9:19:9a:f0:6d:0b:a2:14:6a:c6:4c:c6:                    4e:bd:63:12:a5:0b:4d:97:eb:42:09:79:53:e2:65:                    aa:24:34:70:b8:c1:ab:23:80:e7:9c:6c:ed:dc:82:                    aa:37:04:b8:43:2a:3d:2a:a8:cc:20:fc:27:5d:90:                    26:58:f9:b7:14:e2:9e:e2:c1:70:73:97:e9:6b:02:                    8e:d3:52:59:7b:00:ec:61:30:f1:56:3f:9c:c1:7c:                    05:c5:b1:36:c8:18:85:cf:61:40:1f:07:e8:a7:06:                    87:df:9a:77:0b:a9:64:72:03:f6:93:fc:e0:02:59:                    c1:96:ec:c0:09:42:3e:30:a2:7f:1b:48:2f:fe:e0:                    21:8f:53:87:25:0d:cb:ea:49:f5:4a:9b:d0:e3:5f:                    ee:78:18:e5:ba:71:31:a9:04:98:0f:b1:ad:67:52:                    a0:f2:e3:9c:ab:6a:fe:58:84:84:dd:07:3d:32:94:                    05:16:45:15:96:59:a0:58:6c:18:0e:e3:77:66:c7:                    b3:f7:99                Exponent: 65537 (0x10001)        X509v3 extensions:            X509v3 Basic Constraints:                 CA:FALSE            Netscape Comment:                 OpenSSL Generated Certificate            X509v3 Subject Key Identifier:                 59:5F:C9:13:BA:1B:CC:B9:A8:41:4A:8A:49:79:6A:36:F6:7D:3E:D7            X509v3 Authority Key Identifier:                 keyid:23:6C:2D:3D:3E:29:5D:78:B8:6C:3E:AA:E2:BB:2E:1E:6C:87:F2:53    Signature Algorithm: sha1WithRSAEncryption        13:e7:02:45:3e:a7:ab:bd:b8:da:e7:ef:74:88:ac:62:d5:dd:        10:56:d5:46:07:ec:fa:6a:80:0c:b9:62:be:aa:08:b4:be:0b:        eb:9a:ef:68:b7:69:6f:4d:20:92:9d:18:63:7a:23:f4:48:87:        6a:14:c3:91:98:1b:4e:08:59:3f:91:80:e9:f4:cf:fd:d5:bf:        af:4b:e4:bd:78:09:71:ac:d0:81:e5:53:9f:3e:ac:44:3e:9f:        f0:bf:5a:c1:70:4e:06:04:ef:dc:e8:77:05:a2:7d:c5:fa:80:        58:0a:c5:10:6d:90:ca:49:26:71:84:39:b7:9a:3e:e9:6f:ae:        c5:35:b6:5b:24:8c:c9:ef:41:c3:b1:17:b6:3b:4e:28:89:3c:        7e:87:a8:3a:a5:6d:dc:39:03:20:20:0b:c5:80:a3:79:13:1e:        f6:ec:ae:36:df:40:74:34:87:46:93:3b:a3:e0:a4:8c:2f:43:        4c:b2:54:80:71:76:78:d4:ea:12:28:d8:f2:e3:80:55:11:9b:        f4:65:dc:53:0e:b4:4c:e0:4c:09:b4:dc:a0:80:5c:e6:b5:3b:        95:d3:69:e4:52:3d:5b:61:86:02:e5:fd:0b:00:3a:fa:b3:45:        cc:c9:a3:64:f2:dc:25:59:89:58:0d:9e:6e:28:3a:55:45:50:        5f:88:67:2a:d2:e2:48:cc:8b:de:9a:1b:93:ae:87:e1:f2:90:        50:40:d9:0f:44:31:53:46:ad:62:4e:8d:48:86:19:77:fc:59:        75:91:79:35:59:1d:e3:4e:33:5b:e2:31:d7:ee:52:28:5f:0a:        70:a7:be:bb:1c:03:ca:1a:18:d0:f5:c1:5b:9c:73:04:b6:4a:        e8:46:52:58:76:d4:6a:e6:67:1c:0e:dc:13:d0:61:72:a0:92:        cb:05:97:47:1c:c1:c9:cf:41:7d:1f:b1:4d:93:6b:53:41:03:        21:2b:93:15:63:08:3e:2c:86:9e:7b:9f:3a:09:05:6a:7d:bb:        1c:a7:b7:af:96:08:cb:5b:df:07:fb:9c:f2:95:11:c0:82:81:        f6:1b:bf:5a:1e:58:cd:28:ca:7d:04:eb:aa:e9:29:c4:82:51:        2c:89:61:95:b6:ed:a5:86:7c:7c:48:1d:ec:54:96:47:79:ea:        fc:7f:f5:10:43:0a:9b:00:ef:8a:77:2e:f4:36:66:d2:6a:a6:        95:b6:9f:23:3b:12:e2:89:d5:a4:c1:2c:91:4e:cb:94:e8:3f:        22:0e:21:f9:b8:4a:81:5c:4c:63:ae:3d:05:b2:5c:5c:54:a7:        55:8f:98:25:55:c4:a6:90:bc:19:29:b1:14:d4:e2:b0:95:e4:        ff:89:71:61:be:8a:16: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",
                            ],
                        },
                    },
                },
            },
            QueueConnectionStrings: {
                "kafka-name": {
                    Type: "Queue",
                    Name: "kafka-name",
                    BrokerType: "Kafka",
                    KafkaConnectionSettings: {
                        BootstrapServers: "test:0",
                        UseRavenCertificate: false,
                        ConnectionOptions: {},
                    },
                    RabbitMqConnectionSettings: null,
                },
                "rabbitmq-name": {
                    Type: "Queue",
                    Name: "rabbitmq-name",
                    BrokerType: "RabbitMq",
                    KafkaConnectionSettings: null,
                    RabbitMqConnectionSettings: {
                        ConnectionString: "some-connection-string",
                    },
                },
            },
        };
    }

    static nodeConnectionTestErrorResult(): Raven.Server.Web.System.NodeConnectionTestResult {
        return {
            Success: false,
            HTTPSuccess: false,
            TcpServerUrl: null,
            Log: [],
            Error: "System.UriFormatException: Invalid URI: The format of the URI could not be determined.\n   at System.Uri.CreateThis(String uri, Boolean dontEscape, UriKind uriKind, UriCreationOptions& creationOptions)\n   at System.Uri..ctor(String uriString)\n   at Raven.Server.Documents.ETL.Providers.Queue.QueueBrokerConnectionHelper.CreateRabbitMqConnection(RabbitMqConnectionSettings settings) in D:\\Builds\\RavenDB-6.0-Nightly\\20231123-0200\\src\\Raven.Server\\Documents\\ETL\\Providers\\Queue\\QueueBrokerConnectionHelper.cs:line 80",
        };
    }

    static nodeConnectionTestSuccessResult(): Raven.Server.Web.System.NodeConnectionTestResult {
        return {
            Success: true,
            HTTPSuccess: true,
            TcpServerUrl: null,
            Log: [],
            Error: null,
        };
    }

    static conflictSolverConfiguration(): Raven.Client.ServerWide.ConflictSolver {
        return {
            ResolveByCollection: {
                Categories: {
                    Script: `
var maxRecord = 0;
for (var i = 0; i < docs.length; i++) {
    maxRecord = Math.max(docs[i].MaxRecord, maxRecord);   
}
docs[0].MaxRecord = maxRecord;

return docs[0];`,
                    LastModifiedTime: "2024-01-03T12:13:16.7455603Z",
                },
                Shippers: {
                    Script: `
var maxPrice = 0;
for (var i = 0; i < docs.length; i++) {
    maxPrice = Math.max(docs[i].PricePerUnit, maxPrice);   
}
docs[0].PricePerUnit = maxPrice;

return docs[0];`,
                    LastModifiedTime: "2024-01-04T12:13:16.7455603Z",
                },
            },
            ResolveToLatest: true,
        };
    }

    static databaseRecord(): document {
        return new document({
            DatabaseName: "drec",
            Disabled: false,
            Encrypted: false,
            EtagForBackup: 0,
            DeletionInProgress: {},
            RollingIndexes: {},
            DatabaseState: "Normal",
            LockMode: "Unlock",
            Topology: {
                Members: ["A"],
                Promotables: [],
                Rehabs: [],
                PredefinedMentors: {},
                DemotionReasons: {},
                PromotablesStatus: {},
                Stamp: {
                    Index: 512,
                    Term: 1,
                    LeadersTicks: -2,
                },
                DynamicNodesDistribution: false,
                ReplicationFactor: 1,
                PriorityOrder: [],
                NodesModifiedAt: "2024-01-02T12:47:22.6904463Z",
                DatabaseTopologyIdBase64: "V/OB7JEtLEiazn6QID9RQw",
                ClusterTransactionIdBase64: "VtiBjDGBe0uajuJ7lArnbw",
            },
            Sharding: null,
            ConflictSolverConfig: null,
            DocumentsCompression: {
                Collections: [],
                CompressAllCollections: false,
                CompressRevisions: true,
            },
            Sorters: {},
            Analyzers: {},
            Indexes: {},
            IndexesHistory: {},
            AutoIndexes: {},
            Settings: {},
            Revisions: null,
            TimeSeries: null,
            RevisionsForConflicts: null,
            Expiration: null,
            Refresh: null,
            DataArchival: null,
            Integrations: null,
            PeriodicBackups: [],
            ExternalReplications: [],
            SinkPullReplications: [],
            HubPullReplications: [],
            RavenConnectionStrings: {},
            SqlConnectionStrings: {},
            OlapConnectionStrings: {},
            ElasticSearchConnectionStrings: {},
            QueueConnectionStrings: {},
            RavenEtls: [],
            SqlEtls: [],
            ElasticSearchEtls: [],
            OlapEtls: [],
            QueueEtls: [],
            QueueSinks: [],
            Client: null,
            Studio: null,
            TruncatedClusterTransactionCommandsCount: 0,
            UnusedDatabaseIds: [],
            Etag: 512,
        });
    }
}
