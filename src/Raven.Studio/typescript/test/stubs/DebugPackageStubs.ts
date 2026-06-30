import { IndexesStubs } from "./IndexesStubs";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type DetectedIssue = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.DetectedIssue;
type IssueSeverity = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueSeverity;
type IssueCategory = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueCategory;
type ClusterOverviewPayload = Raven.Server.Dashboard.Cluster.Notifications.ClusterOverviewPayload;
type ClusterObserverDecisions = Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions;
type DatabaseStatistics = Raven.Client.Documents.Operations.DatabaseStatistics;
type IndexStats = Raven.Client.Documents.Indexes.IndexStats;
type IndexDefinition = Raven.Client.Documents.Indexes.IndexDefinition;
type IndexErrors = Raven.Client.Documents.Indexes.IndexErrors;
type OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
type SettingsResult = Raven.Server.Config.SettingsResult;
type NetworkAnalysisInfo = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.NetworkAnalysisInfo;
type ThreadsInfo = Raven.Server.Dashboard.ThreadsInfo;

// Stubs are intentionally partial - they only fill the fields the UI reads. Typing them as DeepPartial of the
// real server type (instead of `as unknown as ...`) keeps that partiality while still failing the build if a
// field we do set is renamed/retyped on the server, so we find out when the server contract changes.
type DeepPartial<T> = T extends (infer U)[]
    ? DeepPartial<U>[]
    : T extends object
      ? { [K in keyof T]?: DeepPartial<T[K]> }
      : T;

// One flag per summary-driven (synchronous) section. Setting a flag strips that section's backing
// data from the built summary so the section renders its no-data state. Async sections (raft log,
// observer decisions, all per-database panels) are emptied separately via the service mocks.
export interface StorySummaryOmit {
    analysisErrors?: boolean;
    issues?: boolean;
    clusterOverview?: boolean;
    resourceUsage?: boolean;
    databasesOverview?: boolean;
    storagePerDatabase?: boolean;
    indexingPerNode?: boolean;
    ongoingTasks?: boolean;
}

const mb = 1024 * 1024;

export class DebugPackageStubs {
    static analysisSummary(): DebugPackageAnalysisSummary {
        return {
            PackageId: "story-package",
            ClusterWideIssues: {
                ServerIssues: [],
                ClusterIssues: [
                    DebugPackageStubs.issue(
                        "Cluster Observer is suspended",
                        "Cluster Observer is suspended",
                        "Warning",
                        "Cluster"
                    ),
                    DebugPackageStubs.issue(
                        "Big number of uncommitted Cluster Log entries",
                        "There are 4 Raft commands left to be committed",
                        "Warning",
                        "Cluster"
                    ),
                ],
                DatabaseIssues: {},
            },
            SummaryPerNode: {
                A: {
                    ClusterNodeInfo: DebugPackageStubs.nodeInfo("A", "Leader", "http://127.0.0.1:8080"),
                    CpuUsageInfo: DebugPackageStubs.cpuInfo(),
                    MemoryUsageInfo: DebugPackageStubs.memoryInfo(),
                    GcInfo: DebugPackageStubs.gcInfo(),
                    DatabasesOverview: DebugPackageStubs.databasesOverview(["Orders", "Products", "Customers"]),
                    DatabaseStorageUsage: DebugPackageStubs.storageUsage([
                        { name: "Orders", size: 512 * mb, temp: 32 * mb },
                        { name: "Products", size: 256 * mb, temp: 16 * mb },
                        { name: "Customers", size: 128 * mb, temp: 8 * mb },
                    ]),
                    DatabaseIndexingSpeed: DebugPackageStubs.indexingSpeed(1250, 3400, 120),
                    DatabasesOngoingTasks: DebugPackageStubs.ongoingTasks({
                        ExternalReplicationCount: 1,
                        PeriodicBackupCount: 1,
                        RavenEtlCount: 2,
                        SubscriptionCount: 3,
                    }),
                    DetectedIssues: {
                        ServerIssues: [
                            DebugPackageStubs.issue(
                                "High managed heap fragmentation",
                                "Managed heap fragmentation was 82.5% when the last full blocking GC has occurred",
                                "Warning",
                                "Server"
                            ),
                            DebugPackageStubs.issue(
                                "High managed memory utilization",
                                "Managed memory usage (19.21 GB) is more than 50% of installed memory (32 GB)",
                                "Info",
                                "Server"
                            ),
                        ],
                        ClusterIssues: [
                            DebugPackageStubs.issue(
                                "Critical error in Cluster Log",
                                "Data corruption detected in Raft log at index 145,882. Unable to apply log entries.",
                                "Error",
                                "Cluster"
                            ),
                        ],
                        DatabaseIssues: {
                            Orders: [
                                DebugPackageStubs.issue(
                                    "High free space detected in documents storage (Orders)",
                                    "Data file /var/lib/ravendb/Databases/Orders/Raven.voron has 73.45% free space.",
                                    "Info",
                                    "Server",
                                    "Consider compacting the storage to reduce its size if you need to free up space"
                                ),
                            ],
                        },
                    },
                } as any,
                B: {
                    ClusterNodeInfo: DebugPackageStubs.nodeInfo("B", "Follower", "http://127.0.0.1:8081"),
                    CpuUsageInfo: DebugPackageStubs.cpuInfo(9, 28),
                    MemoryUsageInfo: DebugPackageStubs.memoryInfo(),
                    GcInfo: DebugPackageStubs.gcInfo(),
                    DatabasesOverview: DebugPackageStubs.databasesOverview(["Orders", "Products", "Customers"]),
                    DatabaseStorageUsage: DebugPackageStubs.storageUsage([
                        { name: "Orders", size: 498 * mb, temp: 30 * mb },
                        { name: "Products", size: 251 * mb, temp: 15 * mb },
                        { name: "Customers", size: 130 * mb, temp: 9 * mb },
                    ]),
                    DatabaseIndexingSpeed: DebugPackageStubs.indexingSpeed(980, 2700, 95),
                    DatabasesOngoingTasks: DebugPackageStubs.ongoingTasks({
                        ExternalReplicationCount: 1,
                        PeriodicBackupCount: 1,
                        RavenEtlCount: 2,
                    }),
                    AnalyzeErrors: {
                        Errors: [
                            {
                                ComponentName: "GcAnalyzer",
                                ErrorMessage: "Failed to parse gc.log: unexpected end of stream",
                                Exception:
                                    "System.IO.InvalidDataException: Unexpected end of stream\n   at Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.GcAnalyzer.Analyze()",
                                Severity: "Warning",
                            },
                        ],
                    },
                    DetectedIssues: {
                        ServerIssues: [],
                        ClusterIssues: [
                            DebugPackageStubs.issue(
                                "Node is in Rehab state",
                                "The cluster node is currently in Rehab state",
                                "Warning",
                                "Cluster"
                            ),
                            DebugPackageStubs.issue(
                                "Cluster Log is stalled on this node",
                                "There was no commit for 2 hours and 35 minutes, while there are 1,847 pending log entries to process",
                                "Error",
                                "Cluster"
                            ),
                        ],
                        DatabaseIssues: {},
                    },
                } as any,
                C: {
                    ClusterNodeInfo: DebugPackageStubs.nodeInfo("C", "Follower", "http://127.0.0.1:8082"),
                    CpuUsageInfo: DebugPackageStubs.cpuInfo(1, 15),
                    MemoryUsageInfo: DebugPackageStubs.memoryInfo(),
                    GcInfo: DebugPackageStubs.gcInfo(),
                    DatabasesOverview: DebugPackageStubs.databasesOverview(["Orders", "Products", "Customers"]),
                    DatabaseStorageUsage: DebugPackageStubs.storageUsage([
                        { name: "Orders", size: 505 * mb, temp: 31 * mb },
                        { name: "Products", size: 248 * mb, temp: 14 * mb },
                        { name: "Customers", size: 126 * mb, temp: 7 * mb },
                    ]),
                    DatabaseIndexingSpeed: DebugPackageStubs.indexingSpeed(1100, 3100, 110),
                    DatabasesOngoingTasks: DebugPackageStubs.ongoingTasks({
                        PeriodicBackupCount: 1,
                        SubscriptionCount: 3,
                    }),
                    DetectedIssues: {
                        ServerIssues: [],
                        ClusterIssues: [
                            DebugPackageStubs.issue(
                                "Cluster node connectivity issue",
                                "Current node is not connected to C (status: Failed to create a connection to node C at " +
                                    "https://c.rdb14548.arek-t3st.cloudtest.ravendb.org. System.AggregateException: One or more " +
                                    "errors occurred. (An exception occurred while contacting " +
                                    "https://c.rdb14548.arek-t3st.cloudtest.ravendb.org/info/tcp?tag=Cluster. " +
                                    "System.Net.Http.HttpRequestException: Connection refused " +
                                    "(c.rdb14548.arek-t3st.cloudtest.ravendb.org:443) ---> System.Net.Sockets.SocketException (111): " +
                                    "Connection refused\n" +
                                    "   at System.Net.Sockets.Socket.AwaitableSocketAsyncEventArgs.System.Threading.Tasks.Sources" +
                                    ".IValueTaskSource.GetResult(Int16 token)\n" +
                                    "   at System.Net.Sockets.Socket.<ConnectAsync>g__WaitForConnectWithCancellation|285_0" +
                                    "(AwaitableSocketAsyncEventArgs saea, ValueTask connectTask, CancellationToken cancellationToken)\n" +
                                    "   at System.Net.Http.HttpConnectionPool.ConnectToTcpHostAsync(String host, Int32 port, " +
                                    "HttpRequestMessage initialRequest, Boolean async, CancellationToken cancellationToken))",
                                "Error",
                                "Cluster"
                            ),
                        ],
                        DatabaseIssues: {},
                    },
                } as any,
            },
        } as DebugPackageAnalysisSummary;
    }

    private static issue(
        title: string,
        description: string,
        severity: IssueSeverity,
        category: IssueCategory,
        recommendedAction?: string
    ): DetectedIssue {
        return {
            Title: title,
            Description: description,
            Severity: severity,
            Category: category,
            RecommendedAction: recommendedAction ?? null,
        };
    }

    private static nodeInfo(tag: string, state: string, url: string): ClusterOverviewPayload {
        return {
            NodeTag: tag,
            NodeState: state,
            NodeType: "Member",
            NodeUrl: url,
            OsName: "Windows",
            OsType: "Windows",
            ServerVersion: "7.1.3",
            StartTime: "2025-10-05T08:00:00.0000000Z",
            UpTime: "45.12:33:00",
        } as ClusterOverviewPayload;
    }

    private static databasesOverview(names: string[]) {
        return {
            Items: names.map(
                (name): Raven.Server.Dashboard.DatabaseInfoItem => ({
                    Database: name,
                    DocumentsCount: 12345,
                    IndexesCount: 4,
                    ErroredIndexesCount: 0,
                    IndexingErrorsCount: 0,
                    Disabled: false,
                    Online: true,
                    Irrelevant: false,
                    OngoingTasksCount: 2,
                    ReplicationFactor: 3,
                    AlertsCount: 0,
                    PerformanceHintsCount: 0,
                    BackupInfo: null,
                })
            ),
        };
    }

    private static storageUsage(items: { name: string; size: number; temp: number }[]) {
        return {
            Items: items.map((item) => ({ Database: item.name, Size: item.size, TempBuffersSize: item.temp })),
        };
    }

    private static indexingSpeed(indexed: number, mapped: number, reduced: number) {
        return { IndexedPerSecond: indexed, MappedPerSecond: mapped, ReducedPerSecond: reduced };
    }

    private static ongoingTasks(counts: Partial<Raven.Server.Dashboard.DatabaseOngoingTasksInfoItem>) {
        const item: Partial<Raven.Server.Dashboard.DatabaseOngoingTasksInfoItem> = { Database: null, ...counts };
        return { Items: [item] };
    }

    private static gen(before: number, after: number) {
        return {
            SizeBeforeBytes: before,
            SizeAfterBytes: after,
            FragmentationBeforeBytes: Math.round(before * 0.05),
            FragmentationAfterBytes: Math.round(after * 0.03),
        };
    }

    private static gcInfo() {
        return {
            Index: 142,
            Generation: 2,
            Concurrent: true,
            Compacted: false,
            PauseTimePercentage: 1,
            PauseDurationsInMs: [12, 8],
            TotalHeapSizeAfterBytes: 2254857830,
            Gen0HeapSize: DebugPackageStubs.gen(512 * mb, 64 * mb),
            Gen1HeapSize: DebugPackageStubs.gen(128 * mb, 96 * mb),
            Gen2HeapSize: DebugPackageStubs.gen(1024 * mb, 980 * mb),
            LargeObjectHeapSize: DebugPackageStubs.gen(256 * mb, 250 * mb),
            PinnedObjectHeapSize: DebugPackageStubs.gen(8 * mb, 8 * mb),
        };
    }

    private static cpuInfo(current = 12, machine = 34) {
        return {
            CurrentCpuUsage: current,
            CurrentMachineCpuUsage: machine,
            AverageCpuUsage: 18,
            KernelTimePercentage: 4,
            NumberOfCores: 8,
            UtilizedCores: 6,
            ProcessorAffinity: 255,
            PrivilegedProcessorTime: "00:12:05",
            TotalProcessorTime: "02:34:11",
            UserProcessorTime: "02:22:06",
            TopCurrentCpuUsageThreads: ["Thread 1234 'Indexing of Orders' - 8%", "Thread 5678 'Raven.Voron' - 3%"],
            TopOverallCpuUsageThreads: ["Thread 1234 'Indexing of Orders' - 12%", "Thread 999 'GC' - 5%"],
        };
    }

    private static memoryInfo() {
        return {
            WorkingSet: "19.21 GB",
            PhysicalMemory: "32 GB",
            AvailableMemory: "11.4 GB",
            AvailableMemoryForProcessing: "10.1 GB",
            DirtyMemory: "256 MB",
            IsHighDirty: false,
            MemoryMapped: "8.3 GB",
            Managed: {
                ManagedAllocations: "2.1 GB",
                LuceneManagedAllocationsForTermCache: "120 MB",
                LastGcInfo: DebugPackageStubs.gcInfo(),
            },
            Unmanaged: {
                UnmanagedAllocations: "1.4 GB",
                EncryptionBuffersInUse: "0 Bytes",
                EncryptionBuffersPool: "0 Bytes",
                EncryptionLockedMemory: "0 Bytes",
                LuceneUnmanagedAllocationsForSorting: "32 MB",
                LuceneUnmanagedAllocationsForTermCache: "64 MB",
                ThreadAllocations: [] as unknown[],
            },
        };
    }

    // ----- Story builders -----

    // Builds a comprehensive summary for `nodeTags.length` nodes with every synchronous section filled.
    // The first node is the leader. `omit` flags strip individual sections so the story controls can
    // empty them one at a time.
    static storySummary(nodeTags: string[], omit: StorySummaryOmit = {}): DebugPackageAnalysisSummary {
        const databases = ["Orders", "Products", "Customers"];
        const summaryPerNode: Record<string, any> = {};

        nodeTags.forEach((tag, index) => {
            const isLeader = index === 0;
            summaryPerNode[tag] = {
                ClusterNodeInfo: omit.clusterOverview
                    ? null
                    : DebugPackageStubs.nodeInfo(
                          tag,
                          isLeader ? "Leader" : "Follower",
                          `http://127.0.0.1:${8080 + index}`
                      ),
                CpuUsageInfo: omit.resourceUsage ? null : DebugPackageStubs.cpuInfo(12 - index, 34 - index * 6),
                MemoryUsageInfo: omit.resourceUsage ? null : DebugPackageStubs.memoryInfo(),
                GcInfo: omit.resourceUsage ? null : DebugPackageStubs.gcInfo(),
                DatabasesOverview: DebugPackageStubs.databasesOverview(omit.databasesOverview ? [] : databases),
                DatabaseStorageUsage: omit.storagePerDatabase
                    ? { Items: [] }
                    : DebugPackageStubs.storageUsage(
                          databases.map((name, i) => ({
                              name,
                              size: Math.round((512 / (i + 1)) * mb),
                              temp: Math.round((32 / (i + 1)) * mb),
                          }))
                      ),
                DatabaseIndexingSpeed: omit.indexingPerNode
                    ? null
                    : DebugPackageStubs.indexingSpeed(1250 - index * 120, 3400 - index * 250, 120 - index * 12),
                DatabasesOngoingTasks: omit.ongoingTasks
                    ? { Items: [] }
                    : DebugPackageStubs.ongoingTasks({
                          ExternalReplicationCount: 1,
                          PeriodicBackupCount: 1,
                          RavenEtlCount: 2,
                          SubscriptionCount: 3,
                      }),
                AnalyzeErrors: { Errors: omit.analysisErrors ? [] : [DebugPackageStubs.analyzeError()] },
                DetectedIssues: DebugPackageStubs.detectedIssues(isLeader, omit.issues),
            };
        });

        return {
            PackageId: "story-package",
            ClusterWideIssues: omit.issues
                ? DebugPackageStubs.emptyIssues()
                : {
                      ServerIssues: [],
                      ClusterIssues: [
                          DebugPackageStubs.issue(
                              "Cluster Observer is suspended",
                              "Cluster Observer is suspended",
                              "Warning",
                              "Cluster"
                          ),
                          DebugPackageStubs.issue(
                              "Big number of uncommitted Cluster Log entries",
                              "There are 4 Raft commands left to be committed",
                              "Warning",
                              "Cluster"
                          ),
                      ],
                      DatabaseIssues: {},
                  },
            SummaryPerNode: summaryPerNode,
        } as DebugPackageAnalysisSummary;
    }

    private static detectedIssues(isLeader: boolean, omit?: boolean) {
        if (omit) {
            return DebugPackageStubs.emptyIssues();
        }
        if (isLeader) {
            return {
                ServerIssues: [
                    DebugPackageStubs.issue(
                        "High managed heap fragmentation",
                        "Managed heap fragmentation was 82.5% when the last full blocking GC has occurred",
                        "Warning",
                        "Server"
                    ),
                    DebugPackageStubs.issue(
                        "High managed memory utilization",
                        "Managed memory usage (19.21 GB) is more than 50% of installed memory (32 GB)",
                        "Info",
                        "Server"
                    ),
                ],
                ClusterIssues: [
                    DebugPackageStubs.issue(
                        "Critical error in Cluster Log",
                        "Data corruption detected in Raft log at index 145,882. Unable to apply log entries.",
                        "Error",
                        "Cluster"
                    ),
                ],
                DatabaseIssues: {
                    Orders: [
                        DebugPackageStubs.issue(
                            "High free space detected in documents storage (Orders)",
                            "Data file /var/lib/ravendb/Databases/Orders/Raven.voron has 73.45% free space.",
                            "Info",
                            "Server",
                            "Consider compacting the storage to reduce its size if you need to free up space"
                        ),
                    ],
                },
            };
        }
        return {
            ServerIssues: [],
            ClusterIssues: [
                DebugPackageStubs.issue(
                    "Node is in Rehab state",
                    "The cluster node is currently in Rehab state",
                    "Warning",
                    "Cluster"
                ),
            ],
            DatabaseIssues: {},
        };
    }

    private static emptyIssues() {
        return { ServerIssues: [] as DetectedIssue[], ClusterIssues: [] as DetectedIssue[], DatabaseIssues: {} };
    }

    private static analyzeError() {
        return {
            ComponentName: "GcAnalyzer",
            ErrorMessage: "Failed to parse gc.log: unexpected end of stream",
            Exception:
                "System.IO.InvalidDataException: Unexpected end of stream\n   at Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.GcAnalyzer.Analyze()",
            Severity: "Warning",
        };
    }

    // ----- On-demand (async) section stubs, resolved through MockManageServerService -----

    static observerDecisions(): ClusterObserverDecisions {
        const stub: DeepPartial<ClusterObserverDecisions> = {
            LeaderNode: "A",
            Term: 5,
            Suspended: false,
            Iteration: 1284,
            ObserverLog: [
                {
                    Date: "2025-11-19T10:50:02.0000000Z",
                    Database: "Orders",
                    Message: "Promoting node B to member for database 'Orders'",
                },
                {
                    Date: "2025-11-19T10:50:18.0000000Z",
                    Database: "Products",
                    Message: "Node C is in rehab for database 'Products' (reason: lagging behind)",
                },
                {
                    Date: "2025-11-19T10:51:40.0000000Z",
                    Database: "Customers",
                    Message: "Moving node C to member for database 'Customers'",
                },
                {
                    Date: "2025-11-19T10:52:05.0000000Z",
                    Database: null,
                    Message: "Cluster topology is stable",
                },
            ],
        };
        return stub as ClusterObserverDecisions;
    }

    static databaseStats(): DatabaseStatistics {
        const stub: DeepPartial<DatabaseStatistics> = {
            CountOfDocuments: 1_250_000,
            CountOfIndexes: 4,
            CountOfAttachments: 320,
            CountOfRevisionDocuments: 8400,
            CountOfDocumentsConflicts: 0,
            CountOfTombstones: 1500,
            CountOfCounterEntries: 240,
            CountOfTimeSeriesSegments: 96,
            SizeOnDisk: { HumaneSize: "512 MBytes", SizeInBytes: 512 * mb },
            TempBuffersSizeOnDisk: { HumaneSize: "32 MBytes", SizeInBytes: 32 * mb },
            DatabaseId: "0fb9f6d4-2e1a-4c2c-9d3a-7f1f0c2b8a11",
            DatabaseChangeVector: "A:1234-3f1f0c2b, B:980-7f1f0c2b",
            LastIndexingTime: "2025-11-19T10:40:12.0000000Z",
            Is64Bit: true,
            StaleIndexes: ["Orders/ByCompany"],
        };
        return stub as DatabaseStatistics;
    }

    static databaseIndexStats(): IndexStats[] {
        return IndexesStubs.getSampleStats();
    }

    static databaseIndexDefinitions(): IndexDefinition[] {
        const stub: DeepPartial<IndexDefinition>[] = [
            {
                Name: "Orders/Totals",
                Type: "MapReduce",
                SourceType: "Documents",
                Priority: "Normal",
                State: "Normal",
                LockMode: "Unlock",
                Maps: [
                    "from order in docs.Orders\nselect new {\n    order.Company,\n    Count = 1,\n    Total = order.Lines.Sum(l => l.PricePerUnit * l.Quantity)\n}",
                ],
                Reduce: "from result in results\ngroup result by result.Company into g\nselect new {\n    Company = g.Key,\n    Count = g.Sum(x => x.Count),\n    Total = g.Sum(x => x.Total)\n}",
                Fields: {},
                Configuration: {},
                AdditionalSources: {},
            },
            {
                Name: "Products/Search",
                Type: "Map",
                SourceType: "Documents",
                Priority: "Normal",
                State: "Normal",
                LockMode: "Unlock",
                Maps: ["from product in docs.Products\nselect new {\n    product.Name,\n    product.Category\n}"],
                Fields: {
                    Name: {
                        Indexing: "Search",
                        Storage: "No",
                        Suggestions: true,
                        TermVector: "No",
                        Analyzer: "StandardAnalyzer",
                    },
                },
                Configuration: { "Indexing.MapTimeoutInSec": "30" },
                AdditionalSources: {},
            },
        ];
        return stub as IndexDefinition[];
    }

    static databaseIndexErrors(): IndexErrors[] {
        return IndexesStubs.getIndexErrorDetails() as IndexErrors[];
    }

    static databaseOngoingTasks(): OngoingTasksResult {
        const stub: DeepPartial<OngoingTasksResult> = {
            SubscriptionsCount: 1,
            PullReplications: [],
            OngoingTasks: [
                {
                    TaskId: 1,
                    TaskType: "Replication",
                    TaskName: "External Replication to DR",
                    TaskState: "Enabled",
                    ResponsibleNode: { NodeTag: "A", NodeUrl: "http://127.0.0.1:8080", ResponsibleNode: "A" },
                    Error: null,
                    PinToMentorNode: false,
                },
                {
                    TaskId: 2,
                    TaskType: "RavenEtl",
                    TaskName: "ETL to Analytics",
                    TaskState: "Enabled",
                    ResponsibleNode: { NodeTag: "B", NodeUrl: "http://127.0.0.1:8081", ResponsibleNode: "B" },
                    Error: null,
                    PinToMentorNode: true,
                },
                {
                    TaskId: 3,
                    TaskType: "Backup",
                    TaskName: "Daily S3 Backup",
                    TaskState: "Disabled",
                    ResponsibleNode: { NodeTag: "A", NodeUrl: "http://127.0.0.1:8080", ResponsibleNode: "A" },
                    Error: null,
                    PinToMentorNode: false,
                },
                {
                    TaskId: 4,
                    TaskType: "Subscription",
                    TaskName: "Orders processing",
                    TaskState: "Enabled",
                    ResponsibleNode: { NodeTag: "C", NodeUrl: "http://127.0.0.1:8082", ResponsibleNode: "C" },
                    Error: null,
                    PinToMentorNode: false,
                },
            ],
        };
        return stub as OngoingTasksResult;
    }

    static databaseSettings(): SettingsResult {
        const setting = (
            key: string,
            category: string,
            description: string,
            defaultValue: string | null,
            opts: { db?: string; server?: string; secured?: boolean }
        ) => ({
            Metadata: {
                Keys: [key],
                Category: category,
                Description: description,
                DefaultValue: defaultValue,
                IsSecured: !!opts.secured,
            },
            DatabaseValues: opts.db != null ? { [key]: { HasValue: true, Value: opts.db, HasAccess: true } } : {},
            ServerValues: opts.server != null ? { [key]: { HasValue: true, Value: opts.server, HasAccess: true } } : {},
        });

        const stub: DeepPartial<SettingsResult> = {
            Settings: [
                setting(
                    "Indexing.MapTimeoutInSec",
                    "Indexing",
                    "The number of seconds a map indexing batch is allowed to run.",
                    "10",
                    { db: "30" }
                ),
                setting("Storage.MaxConcurrentFlushes", "Storage", "Maximum number of concurrent flushes.", "10", {
                    server: "20",
                }),
                setting(
                    "Databases.QueryTimeoutInSec",
                    "Databases",
                    "The time in seconds to wait before canceling a query.",
                    "300",
                    {}
                ),
                setting(
                    "Security.Certificate.Password",
                    "Security",
                    "The (optional) password of the server certificate.",
                    null,
                    { db: "supersecret", secured: true }
                ),
            ],
        };
        return stub as SettingsResult;
    }

    static networkInfo(): NetworkAnalysisInfo {
        const stub: DeepPartial<NetworkAnalysisInfo> = {
            TotalActiveTcpConnections: 42,
            TcpConnections: [
                {
                    TcpState: "Established",
                    NumberOfConnectionsInState: 30,
                    TopConnectionsInState: { "10.0.0.5:38291": 12, "10.0.0.6:38422": 8, "10.0.0.7:38510": 6 },
                },
                {
                    TcpState: "TimeWait",
                    NumberOfConnectionsInState: 8,
                    TopConnectionsInState: { "10.0.0.7:39001": 5 },
                },
                {
                    TcpState: "CloseWait",
                    NumberOfConnectionsInState: 4,
                    TopConnectionsInState: {},
                },
            ],
            PingTestResults: [
                {
                    Url: "http://127.0.0.1:8081",
                    SetupAlive: { Time: 12, Error: null },
                    TcpInfo: { ReceiveTime: 8, Error: null },
                },
                {
                    Url: "http://127.0.0.1:8082",
                    SetupAlive: { Time: 0, Error: "Connection timed out" },
                    TcpInfo: {
                        ReceiveTime: 5200,
                        Error: "No connection could be made because the target machine actively refused it.",
                    },
                },
            ],
        };
        return stub as NetworkAnalysisInfo;
    }

    static threadsInfo(): ThreadsInfo {
        const stub: DeepPartial<ThreadsInfo> = {
            ProcessCpuUsage: 14,
            ThreadsCount: 128,
            DedicatedThreadsCount: 24,
            ActiveCores: 6,
            List: [
                {
                    Id: 1234,
                    Name: "Indexing of Orders/Totals",
                    CpuUsage: 8.2,
                    State: "Running",
                    TotalProcessorTime: "00:12:05",
                    UnmanagedAllocationsInBytes: 8 * mb,
                    IoStats: { ReadBytes: 1 * mb, WriteBytes: 2 * mb },
                },
                {
                    Id: 5678,
                    Name: "Voron Flushing Thread",
                    CpuUsage: 3.1,
                    State: "Wait",
                    TotalProcessorTime: "00:04:32",
                    UnmanagedAllocationsInBytes: 2 * mb,
                    IoStats: { ReadBytes: 0, WriteBytes: 512 * 1024 },
                },
                {
                    Id: 999,
                    Name: ".NET ThreadPool Worker",
                    CpuUsage: 1.0,
                    State: "Running",
                    TotalProcessorTime: "00:01:10",
                    UnmanagedAllocationsInBytes: 0,
                    IoStats: { ReadBytes: 0, WriteBytes: 0 },
                },
            ],
        };
        return stub as ThreadsInfo;
    }
}
