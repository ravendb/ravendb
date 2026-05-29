import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DebugPackageAnalyzer from "./DebugPackageAnalyzer";
import DebugPackageAnalysisView from "./partials/DebugPackageAnalysisView";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type DetectedIssue = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.DetectedIssue;
type ClusterOverviewPayload = Raven.Server.Dashboard.Cluster.Notifications.ClusterOverviewPayload;

export default {
    title: "Pages/Manage Server/Debug Package Analyzer",
    component: DebugPackageAnalyzer,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/0AC6Rm0JBS5FBt3rsRKxOl/Pages---Debug-Package-Analyzer?node-id=576-4496",
        },
    },
} satisfies Meta<typeof DebugPackageAnalyzer>;

export const EmptyState: StoryObj<typeof DebugPackageAnalyzer> = {
    name: "Empty (upload) state",
    render: () => <DebugPackageAnalyzer />,
};

export const ClusterContext: StoryObj<typeof DebugPackageAnalyzer> = {
    name: "Loaded - Cluster context",
    render: () => (
        <div className="flex-window padding-xs">
            <div className="bs5 debug-package-analyzer content-margin">
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
