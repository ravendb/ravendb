import nonShardedDatabase from "models/resources/nonShardedDatabase";
import shardedDatabase from "models/resources/shardedDatabase";
import document from "models/database/documents/document";
import { TimeInSeconds } from "common/constants/timeInSeconds";
import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";
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
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;
import EtlErrors = Raven.Server.Documents.ETL.Stats.TaskErrors;

export class DatabasesStubs {
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
            Indexes: [
                {
                    Name: "Orders/ByCompany",
                    LockMode: "Unlock",
                    Priority: "Normal",
                    Type: "MapReduce",
                    SourceType: "Documents",
                },
                {
                    Name: "Products/ByUnitOnStock",
                    LockMode: "Unlock",
                    Priority: "Normal",
                    Type: "Map",
                    SourceType: "Documents",
                },
                {
                    Name: "Companies/StockPrices/TradeVolumeByMonth",
                    LockMode: "Unlock",
                    Priority: "Normal",
                    Type: "MapReduce",
                    SourceType: "TimeSeries",
                },
                {
                    Name: "Product/Rating",
                    LockMode: "Unlock",
                    Priority: "Normal",
                    Type: "MapReduce",
                    SourceType: "Counters",
                },
                {
                    Name: "Product/Search",
                    LockMode: "Unlock",
                    Priority: "Normal",
                    Type: "Map",
                    SourceType: "Documents",
                },
                {
                    Name: "Orders/Totals",
                    LockMode: "Unlock",
                    Priority: "Normal",
                    Type: "Map",
                    SourceType: "Documents",
                },
                {
                    Name: "Orders/ByShipment/Location",
                    LockMode: "Unlock",
                    Priority: "Normal",
                    Type: "Map",
                    SourceType: "Documents",
                },
            ],
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
            CountOfRemoteAttachments: essential.CountOfAttachments - 9,
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
            PerSubscriptionInfoExtended: [
                {
                    Identifier: "Product/Search",
                    Type: "Documents",
                    Collection: "Products",
                    Etag: 0,
                    NumberOfTombstoneLeft: 0,
                    Types: {
                        Documents: 0,
                        TimeSeries: 0,
                        Counters: 0,
                    },
                    Process: "Index",
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
            SnowflakeConnectionStrings: {},
            AiConnectionStrings: {},
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
            SnowflakeConnectionStrings: {
                "snowflake-name": {
                    Type: "Snowflake",
                    Name: "snowflake-name",
                    ConnectionString: "some-snowflake-connection-string",
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
                        Certificate: null,
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
                    AzureQueueStorageConnectionSettings: null,
                    AmazonSqsConnectionSettings: null,
                },
                "rabbitmq-name": {
                    Type: "Queue",
                    Name: "rabbitmq-name",
                    BrokerType: "RabbitMq",
                    KafkaConnectionSettings: null,
                    RabbitMqConnectionSettings: {
                        ConnectionString: "some-connection-string",
                    },
                    AzureQueueStorageConnectionSettings: null,
                    AmazonSqsConnectionSettings: null,
                },
                "azure-queue-storage-name": {
                    Type: "Queue",
                    Name: "azure-queue-storage-name",
                    BrokerType: "AzureQueueStorage",
                    KafkaConnectionSettings: null,
                    RabbitMqConnectionSettings: null,
                    AzureQueueStorageConnectionSettings: {
                        ConnectionString: "some-connection-string",
                        EntraId: null,
                        Passwordless: null,
                    },
                    AmazonSqsConnectionSettings: null,
                },
                "azure-sqs-name": {
                    Type: "Queue",
                    Name: "azure-sqs-name",
                    BrokerType: "AmazonSqs",
                    KafkaConnectionSettings: null,
                    RabbitMqConnectionSettings: null,
                    AzureQueueStorageConnectionSettings: null,
                    AmazonSqsConnectionSettings: {
                        Basic: {
                            AccessKey: "AKIA123",
                            SecretKey: "this is secret",
                            RegionName: "us-west-2",
                        },
                        Passwordless: false,
                    },
                },
            },
            AiConnectionStrings: {
                "ai-name": {
                    Type: "Ai",
                    Name: "ai-name",
                    Identifier: "some-identifier",
                    ModelType: "TextEmbeddings",
                    GoogleSettings: {
                        ApiKey: "some-api-key",
                        Endpoint: "https://generativelanguage.googleapis.com",
                        Model: "some-model",
                        AiVersion: "V1",
                    },
                    AzureOpenAiSettings: null,
                    HuggingFaceSettings: null,
                    OllamaSettings: null,
                    EmbeddedSettings: null,
                    OpenAiSettings: null,
                    MistralAiSettings: null,
                    VertexSettings: null,
                },
                "ai-name-gen-ai": {
                    Type: "Ai",
                    Name: "ai-name-gen-ai",
                    Identifier: "some-identifier",
                    ModelType: "Chat",
                    GoogleSettings: {
                        ApiKey: "some-api-key",
                        Endpoint: "https://generativelanguage.googleapis.com",
                        Model: "some-model",
                        AiVersion: "V1",
                    },
                    AzureOpenAiSettings: null,
                    HuggingFaceSettings: null,
                    OllamaSettings: null,
                    EmbeddedSettings: null,
                    OpenAiSettings: null,
                    MistralAiSettings: null,
                    VertexSettings: null,
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
            SnowflakeConnectionStrings: {},
            OlapConnectionStrings: {},
            ElasticSearchConnectionStrings: {},
            QueueConnectionStrings: {},
            RavenEtls: [],
            SqlEtls: [],
            SnowflakeEtls: [],
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

    static queryResult(): pagedResultExtended<document> {
        return {
            includes: {},
            items: [
                new document({
                    Company: "companies/85-A",
                    Employee: "employees/5-A",
                    Freight: 32.38,
                    Lines: [
                        {
                            Discount: 0,
                            PricePerUnit: 14,
                            Product: "products/11-A",
                            ProductName: "Queso Cabrales",
                            Quantity: 12,
                        },
                        {
                            Discount: 0,
                            PricePerUnit: 9.8,
                            Product: "products/42-A",
                            ProductName: "Singaporean Hokkien Fried Mee",
                            Quantity: 10,
                        },
                        {
                            Discount: 0,
                            PricePerUnit: 34.8,
                            Product: "products/72-A",
                            ProductName: "Mozzarella di Giovanni",
                            Quantity: 5,
                        },
                    ],
                    OrderedAt: "1996-07-04T00:00:00.0000000",
                    RequireAt: "1996-08-01T00:00:00.0000000",
                    ShipTo: {
                        City: "Reims",
                        Country: "France",
                        Line1: "59 rue de l'Abbaye",
                        Line2: null,
                        Location: {
                            Latitude: 49.25595819999999,
                            Longitude: 4.1547448,
                        },
                        PostalCode: "51100",
                        Region: null,
                    },
                    ShipVia: "shippers/3-A",
                    ShippedAt: "1996-07-16T00:00:00.0000000",
                    "@metadata": {
                        "@collection": "Orders",
                        "@change-vector": "A:81-ZQv/fVpd/Ea9NhnAR6VD4Q",
                        "@flags": "HasRevisions",
                        "@id": "orders/1-A",
                        "@timeseries": [],
                        "@last-modified": "2018-07-27T12:11:53.0447651Z",
                    },
                }),
                new document({
                    Company: "companies/79-A",
                    Employee: "employees/6-A",
                    Freight: 11.61,
                    Lines: [
                        {
                            Discount: 0,
                            PricePerUnit: 18.6,
                            Product: "products/14-A",
                            ProductName: "Tofu",
                            Quantity: 9,
                        },
                        {
                            Discount: 0,
                            PricePerUnit: 42.4,
                            Product: "products/51-A",
                            ProductName: "Manjimup Dried Apples",
                            Quantity: 40,
                        },
                    ],
                    OrderedAt: "1996-07-05T00:00:00.0000000",
                    RequireAt: "1996-08-16T00:00:00.0000000",
                    ShipTo: {
                        City: "Münster",
                        Country: "Germany",
                        Line1: "Luisenstr. 48",
                        Line2: null,
                        Location: {
                            Latitude: 51.6566,
                            Longitude: 7.09044,
                        },
                        PostalCode: "44087",
                        Region: null,
                    },
                    ShipVia: "shippers/1-A",
                    ShippedAt: "1996-07-10T00:00:00.0000000",
                    "@metadata": {
                        "@collection": "Orders",
                        "@change-vector": "A:83-ZQv/fVpd/Ea9NhnAR6VD4Q",
                        "@flags": "HasRevisions",
                        "@id": "orders/2-A",
                        "@timeseries": [],
                        "@last-modified": "2018-07-27T12:11:53.0451613Z",
                    },
                }),
            ],
            totalResultCount: 2,
            additionalResultInfo: {
                Diagnostics: [
                    "Executing 'SetNextReader' with 'reader = Lucene.Net.Index.ReadOnlySegmentReader, docBase = 0' arguments",
                    "Executed 'SetNextReader' with 'reader = Lucene.Net.Index.ReadOnlySegmentReader, docBase = 0' arguments.",
                    "Executing 'Copy' with 'slot = 0, doc = 0' arguments",
                ],
            },
        };
    }

    static integrationsPostgreSqlCredentials(): Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlUsernames {
        return {
            Users: [
                {
                    Username: "user1",
                },
                {
                    Username: "user2",
                },
            ],
        };
    }

    static documentsMetadataByIDPrefix(): metadataAwareDto[] {
        return [
            {
                "@metadata": {
                    "@attachments": [
                        {
                            Name: "image.jpg",
                            Hash: "S5Opbm22FH1LW5SAC3wRb3HA64QM7odd26djlt5cAkM=",
                            ContentType: "image/jpeg",
                            Size: 16958,
                        },
                    ],
                    "@collection": "Categories",
                    "@change-vector": "A:1750-LBq7Ndw2DU+ycZolQrvCxQ",
                    "@flags": "HasAttachments",
                    "@id": "categories/1-A",
                    "@last-modified": "2018-07-27T12:15:47.7253469Z",
                },
            },
            {
                "@metadata": {
                    "@attachments": [
                        {
                            Name: "image.jpg",
                            Hash: "YNLL8N+arOV1ZBP5q0wkeWc8RugEQ7wx3wRhB+xQWaI=",
                            ContentType: "image/jpeg",
                            Size: 36514,
                        },
                    ],
                    "@collection": "Categories",
                    "@change-vector": "A:1753-LBq7Ndw2DU+ycZolQrvCxQ",
                    "@flags": "HasAttachments",
                    "@id": "categories/2-A",
                    "@last-modified": "2018-07-27T12:16:24.1438586Z",
                },
            },
            {
                "@metadata": {
                    "@attachments": [
                        {
                            Name: "image.jpg",
                            Hash: "1QxSMa3tBr+y8wQYNre7E9UJFFVTNWGjVoC+IC+gSSs=",
                            ContentType: "image/jpeg",
                            Size: 47955,
                        },
                    ],
                    "@collection": "Categories",
                    "@change-vector": "A:1756-LBq7Ndw2DU+ycZolQrvCxQ",
                    "@flags": "HasAttachments",
                    "@id": "categories/3-A",
                    "@last-modified": "2018-07-27T12:16:44.1738714Z",
                },
            },
            {
                "@metadata": {
                    "@attachments": [
                        {
                            Name: "image.jpg",
                            Hash: "zBO1hw5HSdn8UYmWJKIXZdn2fdH0QNfzmPU2gSMc5yg=",
                            ContentType: "image/jpeg",
                            Size: 43504,
                        },
                    ],
                    "@collection": "Categories",
                    "@change-vector": "A:1759-LBq7Ndw2DU+ycZolQrvCxQ",
                    "@flags": "HasAttachments",
                    "@id": "categories/4-A",
                    "@last-modified": "2018-07-27T12:17:33.8212726Z",
                },
            },
            {
                "@metadata": {
                    "@attachments": [
                        {
                            Name: "image.jpg",
                            Hash: "EMviKh017Gl7KUZWRecVbuCcXNQcrQ/7EdtnLKt/fgc=",
                            ContentType: "image/jpeg",
                            Size: 55376,
                        },
                    ],
                    "@collection": "Categories",
                    "@change-vector": "A:1762-LBq7Ndw2DU+ycZolQrvCxQ",
                    "@flags": "HasAttachments",
                    "@id": "categories/5-A",
                    "@last-modified": "2018-07-27T12:20:31.8237074Z",
                },
            },
            {
                "@metadata": {
                    "@attachments": [
                        {
                            Name: "image.jpg",
                            Hash: "K37huqcfGCjDC0up0zVte7DAut5YS5K1z1kC+iUmeCI=",
                            ContentType: "image/jpeg",
                            Size: 31219,
                        },
                    ],
                    "@collection": "Categories",
                    "@change-vector": "A:1765-LBq7Ndw2DU+ycZolQrvCxQ",
                    "@flags": "HasAttachments",
                    "@id": "categories/6-A",
                    "@last-modified": "2018-07-27T12:20:49.7774078Z",
                },
            },
            {
                "@metadata": {
                    "@attachments": [
                        {
                            Name: "image.jpg",
                            Hash: "asY7yUHhdgaVoKhivgua0OUSJKXqNDa3Z1uLP9XAocM=",
                            ContentType: "image/jpeg",
                            Size: 61749,
                        },
                    ],
                    "@collection": "Categories",
                    "@change-vector": "A:1768-LBq7Ndw2DU+ycZolQrvCxQ",
                    "@flags": "HasAttachments",
                    "@id": "categories/7-A",
                    "@last-modified": "2018-07-27T12:21:11.2283909Z",
                },
            },
            {
                "@metadata": {
                    "@attachments": [
                        {
                            Name: "image.jpg",
                            Hash: "GWdpGVCWyLsrtNdA5AOee0QOZFG6rKIqCosZZN5WnCA=",
                            ContentType: "image/jpeg",
                            Size: 33396,
                        },
                    ],
                    "@collection": "Categories",
                    "@change-vector": "A:1771-LBq7Ndw2DU+ycZolQrvCxQ",
                    "@flags": "HasAttachments",
                    "@id": "categories/8-A",
                    "@last-modified": "2018-07-27T12:21:39.1315788Z",
                },
            },
            {
                "@metadata": {
                    "@collection": "Companies",
                    "@timeseries": ["StockPrices"],
                    "@change-vector": "A:2368-LBq7Ndw2DU+ycZolQrvCxQ",
                    "@flags": "HasTimeSeries",
                    "@id": "companies/1-A",
                    "@last-modified": "2024-06-18T18:58:53.7547407Z",
                },
            },
            {
                "@metadata": {
                    "@collection": "Companies",
                    "@timeseries": ["StockPrices"],
                    "@change-vector": "A:2431-LBq7Ndw2DU+ycZolQrvCxQ",
                    "@flags": "HasTimeSeries",
                    "@id": "companies/10-A",
                    "@last-modified": "2024-06-18T18:58:53.7577639Z",
                },
            },
        ];
    }

    static getIdentities(length = 2): Record<string, number> {
        const object: Record<string, number> = {};
        for (let i = 1; i <= length; i++) {
            const identity = {
                [`key${i}`]: i * 1000,
            };

            Object.assign(object, identity);
        }
        return object;
    }

    static revisionsBinCleaner(): Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration {
        return {
            Disabled: false,
            MinimumEntriesAgeToKeepInMin: 1,
            CleanerFrequencyInSec: 5 * TimeInSeconds.Minute,
        };
    }

    static revisionsPreview(): pagedResultWithToken<RevisionsPreviewResultItem> {
        return {
            items: [
                {
                    ChangeVector: "A:2568-F9I6Egqwm0Kz+K0oFVIR9Q",
                    Collection: "Docs",
                    Id: "doc/1-A",
                    Etag: "2568",
                    Flags: "HasRevisions, DeleteRevision",
                    LastModified: "2018-07-27T12:11:53.0447651Z",
                    ShardNumber: null,
                },
                {
                    ChangeVector: "A:2569-F9I6Egqwm0Kz+K0oFVIR9Q",
                    Collection: "Docs",
                    Id: "doc/2-A",
                    Etag: "2569",
                    Flags: "HasRevisions",
                    LastModified: "2018-07-27T12:11:53.0451613Z",
                    ShardNumber: null,
                },
            ],
            totalResultCount: 2,
        };
    }

    private static generateIdentifier(): string {
        return crypto.randomUUID();
    }

    private static generateDestinations(count: number) {
        const destinations: Record<
            string,
            Raven.Client.Documents.Attachments.RemoteAttachmentsDestinationConfiguration
        > = {};

        for (let i = 0; i < count; i++) {
            const id = this.generateIdentifier();
            const name = `Destination_${i + 1}` + id;

            destinations[name] = {
                Disabled: false,
                S3Settings: {
                    BucketName: "test",
                    StorageClass: "GlacierInstantRetrieval",
                    AwsSessionToken: "test",
                    AwsAccessKey: "test",
                    AwsSecretKey: "test",
                    AwsRegionName: "eu-central-1",
                    CustomServerUrl: "",
                    ForcePathStyle: false,
                    RemoteFolderName: "",
                },
                AzureSettings: {
                    AccountKey: "",
                    AccountName: "",
                    RemoteFolderName: "",
                    SasToken: "",
                    StorageContainer: "",
                },
            };
        }

        return destinations;
    }

    static remoteAttachmentsConfiguration(): Raven.Client.Documents.Attachments.RemoteAttachmentsConfiguration {
        return {
            Disabled: false,
            Destinations: this.generateDestinations(5),
            MaxItemsToProcess: 32,
            CheckFrequencyInSec: 5 * TimeInSeconds.Minute,
        };
    }

    static emptyRemoteAttachmentsConfiguration(): Raven.Client.Documents.Attachments.RemoteAttachmentsConfiguration {
        return {
            Disabled: true,
            MaxItemsToProcess: null,
            CheckFrequencyInSec: null,
            Destinations: {},
        };
    }

    static schemaValidators(): Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration {
        return {
            Disabled: false,
            ValidatorsPerCollection: {
                TestObjs: {
                    Disabled: false,
                    Schema: '{"properties":{"Prop":{"const":"12212213"}}}',
                    LastModifiedTime: "2025-09-19T14:08:13.2913345Z",
                },
                UserProfiles: {
                    Disabled: false,
                    Schema: '{"properties":{"Username":{"type":"string"},"Email":{"format":"email"}}}',
                    LastModifiedTime: "2025-09-20T09:22:47.1234567Z",
                },
                Orders: {
                    Disabled: false,
                    Schema: '{"properties":{"OrderId":{"type":"string"},"Amount":{"type":"number"}}}',
                    LastModifiedTime: "2025-09-18T11:15:33.9876543Z",
                },
                Products: {
                    Disabled: false,
                    Schema: '{"properties":{"ProductName":{"type":"string"},"Price":{"type":"number","minimum":0}}}',
                    LastModifiedTime: "2025-09-21T07:45:00.0000000Z",
                },
                Logs: {
                    Disabled: false,
                    Schema: '{"properties":{"Timestamp":{"type":"string","format":"date-time"},"Message":{"type":"string"}}}',
                    LastModifiedTime: "2025-09-22T13:00:00.1234567Z",
                },
                Inventory: {
                    Disabled: false,
                    Schema: '{"properties":{"ItemId":{"type":"string"},"Quantity":{"type":"integer","minimum":0}}}',
                    LastModifiedTime: "2025-09-17T16:30:25.6543210Z",
                },
                Payments: {
                    Disabled: false,
                    Schema: '{"properties":{"PaymentId":{"type":"string"},"Amount":{"type":"number","minimum":0}}}',
                    LastModifiedTime: "2025-09-23T10:00:00.0000000Z",
                },
                Shipments: {
                    Disabled: false,
                    Schema: '{"properties":{"ShipmentId":{"type":"string"},"Status":{"enum":["pending","shipped","delivered"]}}}',
                    LastModifiedTime: "2025-09-22T08:15:22.1234567Z",
                },
                Reviews: {
                    Disabled: false,
                    Schema: '{"properties":{"ReviewId":{"type":"string"},"Rating":{"type":"integer","minimum":1,"maximum":5}}}',
                    LastModifiedTime: "2025-09-21T11:45:12.6543210Z",
                },
                Coupons: {
                    Disabled: false,
                    Schema: '{"properties":{"CouponCode":{"type":"string"},"Discount":{"type":"number","minimum":0}}}',
                    LastModifiedTime: "2025-09-20T09:00:00.0000000Z",
                },
                Categories: {
                    Disabled: false,
                    Schema: '{"properties":{"CategoryId":{"type":"string"},"CategoryName":{"type":"string"}}}',
                    LastModifiedTime: "2025-09-19T14:30:00.1234567Z",
                },
                Tags: {
                    Disabled: false,
                    Schema: '{"properties":{"TagId":{"type":"string"},"TagName":{"type":"string"}}}',
                    LastModifiedTime: "2025-09-18T17:22:15.9876543Z",
                },
                Sessions: {
                    Disabled: false,
                    Schema: '{"properties":{"SessionId":{"type":"string"},"UserId":{"type":"string"},"ExpiresAt":{"type":"string","format":"date-time"}}}',
                    LastModifiedTime: "2025-09-22T12:00:00.0000000Z",
                },
                Notifications: {
                    Disabled: false,
                    Schema: '{"properties":{"NotificationId":{"type":"string"},"Message":{"type":"string"},"Read":{"type":"boolean"}}}',
                    LastModifiedTime: "2025-09-21T09:30:00.0000000Z",
                },
                Settings: {
                    Disabled: false,
                    Schema: '{"properties":{"SettingKey":{"type":"string"},"SettingValue":{"type":"string"}}}',
                    LastModifiedTime: "2025-09-20T15:45:00.0000000Z",
                },
                LogsArchive: {
                    Disabled: false,
                    Schema: '{"properties":{"LogId":{"type":"string"},"ArchivedAt":{"type":"string","format":"date-time"}}}',
                    LastModifiedTime: "2025-09-19T13:00:00.0000000Z",
                },
                AuditTrails: {
                    Disabled: false,
                    Schema: '{"properties":{"Action":{"type":"string"},"PerformedAt":{"type":"string","format":"date-time"}}}',
                    LastModifiedTime: "2025-09-23T08:00:00.0000000Z",
                },
                Feedback: {
                    Disabled: false,
                    Schema: '{"properties":{"FeedbackId":{"type":"string"},"Comment":{"type":"string"},"Rating":{"type":"integer","minimum":1,"maximum":5}}}',
                    LastModifiedTime: "2025-09-22T14:45:00.0000000Z",
                },
                LogsTemp: {
                    Disabled: false,
                    Schema: '{"properties":{"TempLogId":{"type":"string"},"Data":{"type":"string"}}}',
                    LastModifiedTime: "2025-09-21T18:15:00.0000000Z",
                },
                Events: {
                    Disabled: false,
                    Schema: '{"properties":{"EventId":{"type":"string"},"EventName":{"type":"string"},"Date":{"type":"string","format":"date-time"}}}',
                    LastModifiedTime: "2025-09-20T11:00:00.0000000Z",
                },
                Analytics: {
                    Disabled: false,
                    Schema: '{"properties":{"Metric":{"type":"string"},"Value":{"type":"number"}}}',
                    LastModifiedTime: "2025-09-19T09:00:00.0000000Z",
                },
                Messages: {
                    Disabled: false,
                    Schema: '{"properties":{"MessageId":{"type":"string"},"Content":{"type":"string"},"SentAt":{"type":"string","format":"date-time"}}}',
                    LastModifiedTime: "2025-09-18T22:00:00.0000000Z",
                },
                Tasks: {
                    Disabled: false,
                    Schema: '{"properties":{"TaskId":{"type":"string"},"Description":{"type":"string"},"DueDate":{"type":"string","format":"date-time"}}}',
                    LastModifiedTime: "2025-09-17T20:00:00.0000000Z",
                },
                Subscriptions: {
                    Disabled: false,
                    Schema: '{"properties":{"SubscriptionId":{"type":"string"},"Plan":{"type":"string"},"StartDate":{"type":"string","format":"date-time"}}}',
                    LastModifiedTime: "2025-09-22T07:00:00.0000000Z",
                },
                AuditLogs: {
                    Disabled: false,
                    Schema: '{"properties":{"LogId":{"type":"string"},"Action":{"type":"string"},"PerformedBy":{"type":"string"}}}',
                    LastModifiedTime: "2025-09-21T07:30:00.0000000Z",
                },
            },
        };
    }

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

    private static statusOk(): DatabaseGroupNodeStatus {
        return {
            LastStatus: "Ok",
            LastError: null,
        };
    }

    static etlErrors(): EtlErrors[] {
        return [
            {
                ProcessErrors: [],
                EtlSubType: null,
                ItemErrors: [
                    {
                        TaskName: "ETL1/Transformation1",
                        CreatedAt: "2026-03-09T09:52:12.6923003",
                        Step: "Transformation",
                        Error: 'Raven.Client.Exceptions.Documents.Patching.JavaScriptException: At 11:6 Script failed for document ID \'\'. {"message":"dummy error","stack":"   at call :6:12\\r\\n   at execute (doc, args) :13:5\\r\\n   at :15:1"}\r\n ---> Jint.Runtime.JavaScriptException: dummy error\r\n ---> Error: dummy error\r\n   at call :6:12\r\n   at execute (doc, args) :13:5\r\n   at :15:1\r\n   --- End of inner exception stack trace ---\r\n   at Jint.Runtime.ExceptionHelper.ThrowJavaScriptException(Engine engine, JsValue value, Completion& result)\r\n   at Jint.Native.Function.ScriptFunction.Call(JsValue thisObject, JsValue[] arguments)\r\n   at Jint.Engine.Call(Function function, JsValue thisObject, JsValue[] arguments, JintExpression expression)\r\n   at Jint.Engine.<>c__DisplayClass135_0.<Call>g__Callback|0()\r\n   at Jint.Engine.ExecuteWithConstraints[T](Boolean strict, Func`1 callback)\r\n   at Jint.Engine.Call(JsValue callable, JsValue thisObject, JsValue[] arguments)\r\n   at Raven.Server.Documents.Patch.ScriptRunner.SingleRun.Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, String method, String documentId, Object[] args, QueryTimingsScope scope, CancellationToken token)\r\n   --- End of inner exception stack trace ---\r\n   at Raven.Server.Documents.Patch.ScriptRunner.SingleRun.Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, String method, String documentId, Object[] args, QueryTimingsScope scope, CancellationToken token)\r\n   at Raven.Server.Documents.Patch.ScriptRunner.SingleRun.Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, String method, Object[] args, QueryTimingsScope scope, CancellationToken token)\r\n   at Raven.Server.Documents.ETL.Providers.Raven.RavenEtlDocumentTransformer.Transform(RavenEtlItem item, EtlStatsScope stats, EtlProcessState state)\r\n   at Raven.Server.Documents.ETL.EtlProcess`6.Transform(IEnumerable`1 items, DocumentsOperationContext context, TStatsScope stats, EtlProcessState state)',
                        DocumentId: "users/1000-A",
                    },
                ],
                TaskName: "ETL1/Transformation1",
            },
        ];
    }

    static etlStats(): EtlTaskStats[] {
        return [
            {
                TaskName: "ETL1",
                NodeTag: "A",
                Stats: [
                    {
                        TransformationName: "Transformation1",
                        Statistics: {
                            LastLoadErrorTime: null,
                            LastProcessedEtag: 0,
                            TransformationSuccesses: 0,
                            TransformationErrors: 1000,
                            LoadSuccesses: 0,
                            LoadErrors: 0,
                            LoadSuccessesInCurrentBatch: 0,
                            AverageErrorsRatio: {},
                            HealthStatus: "Failed",
                            NextBatchRetryTime: null,
                            LastSuccessfulBatchTime: null,
                            BatchStopReason: null,
                            LastChangeVector: null,
                            LastSlowSqlWarningsInCurrentBatch: null,
                            WasLatestLoadSuccessful: false,
                        },
                    },
                ],
                EtlType: "Raven",
                TaskId: 1,
                EtlSubType: null,
            },
        ];
    }
}
