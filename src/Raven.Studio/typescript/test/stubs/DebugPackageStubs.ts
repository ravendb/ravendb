type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type DetectedIssue = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.DetectedIssue;
type IssueSeverity = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueSeverity;
type IssueCategory = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueCategory;
type ClusterOverviewPayload = Raven.Server.Dashboard.Cluster.Notifications.ClusterOverviewPayload;

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
                                "Current node is not connected to Node B (status: ConnectionError)",
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
}
