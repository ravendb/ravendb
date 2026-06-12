import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DebugPackage from "./DebugPackage";
import DebugPackageAnalysisView from "./partials/DebugPackageAnalysisView";
import { AboutViewHeading } from "components/common/AboutView";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type DetectedIssue = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.DetectedIssue;
type ClusterOverviewPayload = Raven.Server.Dashboard.Cluster.Notifications.ClusterOverviewPayload;

export default {
    title: "Pages/Manage Server/Debug Package Analyzer",
    component: DebugPackage,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/0AC6Rm0JBS5FBt3rsRKxOl/Pages---Debug-Package-Analyzer?node-id=576-4496",
        },
    },
} satisfies Meta<typeof DebugPackage>;

export const EmptyState: StoryObj<typeof DebugPackage> = {
    name: "Empty (upload) state",
    render: () => <DebugPackage />,
};

export const ClusterContext: StoryObj<typeof DebugPackage> = {
    name: "Loaded - Cluster context",
    render: () => (
        <div className="flex-window padding-xs">
            <div className="bs5 debug-package-analyzer content-margin">
                <AboutViewHeading
                    title="Debug Package Analyzer"
                    backUrl="#admin/settings/debugPackage"
                    marginBottom={1}
                />
                <p className="text-muted fs-5 mb-4">Examine the package to identify the problem with your server</p>
                <DebugPackageAnalysisView
                    summary={mockSummary()}
                    fileName="2025-11-19 10-55-11 Cluster Wide.zip"
                    onReset={() => undefined}
                />
            </div>
        </div>
    ),
};

function issue(title: string, description: string, severity: string, category: string, recommendedAction?: string) {
    return {
        Title: title,
        Description: description,
        Severity: severity,
        Category: category,
        RecommendedAction: recommendedAction ?? null,
    } as DetectedIssue;
}

function nodeInfo(tag: string, state: string, url: string): ClusterOverviewPayload {
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

function databasesOverview(names: string[]) {
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

function storageUsage(items: { name: string; size: number; temp: number }[]) {
    return {
        Items: items.map((item) => ({ Database: item.name, Size: item.size, TempBuffersSize: item.temp })),
    };
}

function indexingSpeed(indexed: number, mapped: number, reduced: number) {
    return { IndexedPerSecond: indexed, MappedPerSecond: mapped, ReducedPerSecond: reduced };
}

function ongoingTasks(counts: Partial<Raven.Server.Dashboard.DatabaseOngoingTasksInfoItem>) {
    const item: Partial<Raven.Server.Dashboard.DatabaseOngoingTasksInfoItem> = { Database: null, ...counts };
    return { Items: [item] };
}

const mb = 1024 * 1024;

function gen(before: number, after: number) {
    return {
        SizeBeforeBytes: before,
        SizeAfterBytes: after,
        FragmentationBeforeBytes: Math.round(before * 0.05),
        FragmentationAfterBytes: Math.round(after * 0.03),
    };
}

function gcInfo() {
    return {
        Index: 142,
        Generation: 2,
        Concurrent: true,
        Compacted: false,
        PauseTimePercentage: 1,
        PauseDurationsInMs: [12, 8],
        TotalHeapSizeAfterBytes: 2254857830,
        Gen0HeapSize: gen(512 * mb, 64 * mb),
        Gen1HeapSize: gen(128 * mb, 96 * mb),
        Gen2HeapSize: gen(1024 * mb, 980 * mb),
        LargeObjectHeapSize: gen(256 * mb, 250 * mb),
        PinnedObjectHeapSize: gen(8 * mb, 8 * mb),
    };
}

function cpuInfo(current = 12, machine = 34) {
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

function memoryInfo() {
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
            LastGcInfo: gcInfo(),
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

function mockSummary(): DebugPackageAnalysisSummary {
    return {
        PackageId: "story-package",
        ClusterWideIssues: {
            ServerIssues: [],
            ClusterIssues: [
                issue("Cluster Observer is suspended", "Cluster Observer is suspended", "Warning", "Cluster"),
                issue(
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
                ClusterNodeInfo: nodeInfo("A", "Leader", "http://127.0.0.1:8080"),
                CpuUsageInfo: cpuInfo(),
                MemoryUsageInfo: memoryInfo(),
                GcInfo: gcInfo(),
                DatabasesOverview: databasesOverview(["Orders", "Products", "Customers"]),
                DatabaseStorageUsage: storageUsage([
                    { name: "Orders", size: 512 * mb, temp: 32 * mb },
                    { name: "Products", size: 256 * mb, temp: 16 * mb },
                    { name: "Customers", size: 128 * mb, temp: 8 * mb },
                ]),
                DatabaseIndexingSpeed: indexingSpeed(1250, 3400, 120),
                DatabasesOngoingTasks: ongoingTasks({
                    ExternalReplicationCount: 1,
                    PeriodicBackupCount: 1,
                    RavenEtlCount: 2,
                    SubscriptionCount: 3,
                }),
                DetectedIssues: {
                    ServerIssues: [
                        issue(
                            "High managed heap fragmentation",
                            "Managed heap fragmentation was 82.5% when the last full blocking GC has occurred",
                            "Warning",
                            "Server"
                        ),
                        issue(
                            "High managed memory utilization",
                            "Managed memory usage (19.21 GB) is more than 50% of installed memory (32 GB)",
                            "Info",
                            "Server"
                        ),
                    ],
                    ClusterIssues: [
                        issue(
                            "Critical error in Cluster Log",
                            "Data corruption detected in Raft log at index 145,882. Unable to apply log entries.",
                            "Error",
                            "Cluster"
                        ),
                    ],
                    DatabaseIssues: {
                        Orders: [
                            issue(
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
                ClusterNodeInfo: nodeInfo("B", "Follower", "http://127.0.0.1:8081"),
                CpuUsageInfo: cpuInfo(9, 28),
                MemoryUsageInfo: memoryInfo(),
                GcInfo: gcInfo(),
                DatabasesOverview: databasesOverview(["Orders", "Products", "Customers"]),
                DatabaseStorageUsage: storageUsage([
                    { name: "Orders", size: 498 * mb, temp: 30 * mb },
                    { name: "Products", size: 251 * mb, temp: 15 * mb },
                    { name: "Customers", size: 130 * mb, temp: 9 * mb },
                ]),
                DatabaseIndexingSpeed: indexingSpeed(980, 2700, 95),
                DatabasesOngoingTasks: ongoingTasks({
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
                        issue(
                            "Node is in Rehab state",
                            "The cluster node is currently in Rehab state",
                            "Warning",
                            "Cluster"
                        ),
                        issue(
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
                ClusterNodeInfo: nodeInfo("C", "Follower", "http://127.0.0.1:8082"),
                CpuUsageInfo: cpuInfo(1, 15),
                MemoryUsageInfo: memoryInfo(),
                GcInfo: gcInfo(),
                DatabasesOverview: databasesOverview(["Orders", "Products", "Customers"]),
                DatabaseStorageUsage: storageUsage([
                    { name: "Orders", size: 505 * mb, temp: 31 * mb },
                    { name: "Products", size: 248 * mb, temp: 14 * mb },
                    { name: "Customers", size: 126 * mb, temp: 7 * mb },
                ]),
                DatabaseIndexingSpeed: indexingSpeed(1100, 3100, 110),
                DatabasesOngoingTasks: ongoingTasks({
                    PeriodicBackupCount: 1,
                    SubscriptionCount: 3,
                }),
                DetectedIssues: {
                    ServerIssues: [],
                    ClusterIssues: [
                        issue(
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
