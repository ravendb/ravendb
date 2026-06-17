import React from "react";
import classNames from "classnames";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import DebugPackage from "./DebugPackage";
import DebugPackageAnalysisView from "./partials/DebugPackageAnalysisView";
import DebugPackageUpload from "./partials/DebugPackageUpload";
import AnalysisError from "./partials/AnalysisError";
import { AboutViewHeading } from "components/common/AboutView";
import { DebugPackageStubs, StorySummaryOmit } from "test/stubs/DebugPackageStubs";
import AnalysisVerdict from "./partials/AnalysisVerdict";
import PackageSummary from "./partials/PackageSummary";
import { FlatIssue, flattenIssues } from "./partials/analyzerUtils";

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

function UploadChrome({ fill, children }: { fill?: boolean; children: React.ReactNode }) {
    return (
        <div className={classNames("flex-window padding-xs", { "debug-package-window-fill": fill })}>
            <div
                className={classNames("bs5 debug-package-analyzer content-margin", {
                    "debug-package-analyzer--fill": fill,
                })}
            >
                <div className="d-flex justify-content-between align-items-start gap-3 flex-wrap mb-4">
                    <div>
                        <AboutViewHeading
                            title="Debug Package Analyzer"
                            icon="gather-debug-information"
                            iconAddon="search"
                            backUrl="#admin/settings/debugPackage"
                            marginBottom={1}
                        />
                        <p className="text-muted fs-5 mb-0">
                            Examine the package to identify the problem with your server
                        </p>
                    </div>
                </div>
                {children}
            </div>
        </div>
    );
}

const uploadError = {
    responseText: JSON.stringify({
        Message: "Failed to analyze the debug package: the archive is not a valid RavenDB debug package.",
        Error:
            "System.IO.InvalidDataException: Central Directory corrupt - the .zip could not be read.\n" +
            "   at System.IO.Compression.ZipArchive.ReadEndOfCentralDirectory()\n" +
            "   at Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalyzer.Analyze()",
    }),
};

type UploadState = "Empty" | "Uploading" | "Upload failed";

export const Upload: StoryObj<{ state: UploadState }> = {
    args: { state: "Empty" },
    argTypes: {
        state: {
            name: "Upload state",
            control: "select",
            options: ["Empty", "Uploading", "Upload failed"] satisfies UploadState[],
        },
    },
    render: ({ state }) => {
        if (state === "Empty") {
            return <DebugPackage />;
        }
        if (state === "Upload failed") {
            return (
                <UploadChrome>
                    <AnalysisError error={uploadError} onReset={() => undefined} />
                </UploadChrome>
            );
        }
        return (
            <UploadChrome fill>
                <DebugPackageUpload
                    isAnalyzing
                    fileName="2025-11-19 10-55-11 Cluster Wide.zip"
                    onFileSelected={() => undefined}
                />
            </UploadChrome>
        );
    },
};

interface SectionToggles {
    // Cluster scope
    showAnalysisErrors: boolean;
    showAnalysisResults: boolean;
    showClusterOverview: boolean;
    showResourceUsage: boolean;
    showDatabasesOverview: boolean;
    showStoragePerDatabase: boolean;
    showIndexingPerNode: boolean;
    showOngoingTasks: boolean;
    showRaftDebug: boolean;
    showObserverDecisions: boolean;
    // Node scope
    showNetworkInfo: boolean;
    showThreadsInfo: boolean;
    // Database scope
    showDatabaseStats: boolean;
    showDatabaseIndexStats: boolean;
    showDatabaseIndexDefinitions: boolean;
    showDatabaseIndexErrors: boolean;
    showDatabaseOngoingTasks: boolean;
    showDatabaseSettings: boolean;
}

const defaultSectionArgs: SectionToggles = {
    showAnalysisErrors: false,
    showAnalysisResults: true,
    showClusterOverview: true,
    showResourceUsage: true,
    showDatabasesOverview: true,
    showStoragePerDatabase: true,
    showIndexingPerNode: true,
    showOngoingTasks: true,
    showRaftDebug: true,
    showObserverDecisions: true,
    showNetworkInfo: true,
    showThreadsInfo: true,
    showDatabaseStats: true,
    showDatabaseIndexStats: true,
    showDatabaseIndexDefinitions: true,
    showDatabaseIndexErrors: true,
    showDatabaseOngoingTasks: true,
    showDatabaseSettings: true,
};

const clusterSections = "Cluster sections";
const nodeSections = "Node sections";
const databaseSections = "Database sections";

const sectionArgTypes = {
    showAnalysisErrors: { name: "Analysis errors", control: "boolean", table: { category: clusterSections } },
    showAnalysisResults: {
        name: "Analysis results (issues)",
        control: "boolean",
        table: { category: clusterSections },
    },
    showClusterOverview: {
        name: "Cluster / Node overview",
        control: "boolean",
        table: { category: clusterSections },
    },
    showResourceUsage: {
        name: "Resource usage / Perf. metrics",
        control: "boolean",
        table: { category: clusterSections },
    },
    showDatabasesOverview: { name: "Databases overview", control: "boolean", table: { category: clusterSections } },
    showStoragePerDatabase: { name: "Storage per database", control: "boolean", table: { category: clusterSections } },
    showIndexingPerNode: { name: "Indexing per node", control: "boolean", table: { category: clusterSections } },
    showOngoingTasks: { name: "Ongoing tasks (summary)", control: "boolean", table: { category: clusterSections } },
    showRaftDebug: { name: "Raft debug", control: "boolean", table: { category: clusterSections } },
    showObserverDecisions: { name: "Observer decisions", control: "boolean", table: { category: clusterSections } },
    showNetworkInfo: { name: "Network info", control: "boolean", table: { category: nodeSections } },
    showThreadsInfo: { name: "Threads info", control: "boolean", table: { category: nodeSections } },
    showDatabaseStats: { name: "Database stats", control: "boolean", table: { category: databaseSections } },
    showDatabaseIndexStats: { name: "Index stats", control: "boolean", table: { category: databaseSections } },
    showDatabaseIndexDefinitions: {
        name: "Index definitions",
        control: "boolean",
        table: { category: databaseSections },
    },
    showDatabaseIndexErrors: { name: "Index errors", control: "boolean", table: { category: databaseSections } },
    showDatabaseOngoingTasks: {
        name: "Ongoing tasks (per database)",
        control: "boolean",
        table: { category: databaseSections },
    },
    showDatabaseSettings: { name: "Settings", control: "boolean", table: { category: databaseSections } },
} as const;

function renderAnalyzer(nodeTags: string[], args: SectionToggles) {
    const omit: StorySummaryOmit = {
        analysisErrors: !args.showAnalysisErrors,
        issues: !args.showAnalysisResults,
        clusterOverview: !args.showClusterOverview,
        resourceUsage: !args.showResourceUsage,
        databasesOverview: !args.showDatabasesOverview,
        storagePerDatabase: !args.showStoragePerDatabase,
        indexingPerNode: !args.showIndexingPerNode,
        ongoingTasks: !args.showOngoingTasks,
    };
    const summary = DebugPackageStubs.storySummary(nodeTags, omit);

    // The async sections fetch from manageServerService; configure each mock to resolve either a
    // filled stub or its empty value based on the matching control (off = empty).
    const { manageServerService } = mockServices;
    manageServerService.withDebugPackageClusterLog(!args.showRaftDebug);
    manageServerService.withDebugPackageClusterObserverDecisions(!args.showObserverDecisions);
    manageServerService.withDebugPackageNetworkInfo(!args.showNetworkInfo);
    manageServerService.withDebugPackageThreadsInfo(!args.showThreadsInfo);
    manageServerService.withDebugPackageDatabaseStats(!args.showDatabaseStats);
    manageServerService.withDebugPackageDatabaseIndexStats(!args.showDatabaseIndexStats);
    manageServerService.withDebugPackageDatabaseIndexDefinitions(!args.showDatabaseIndexDefinitions);
    manageServerService.withDebugPackageDatabaseIndexErrors(!args.showDatabaseIndexErrors);
    manageServerService.withDebugPackageDatabaseOngoingTasks(!args.showDatabaseOngoingTasks);
    manageServerService.withDebugPackageDatabaseSettings(!args.showDatabaseSettings);

    // The async panels cache their fetch (useAsync keyed by packageId/node/database), so toggling a
    // control would not re-fetch. Remount the whole view via a key derived from the args + node set.
    const remountKey =
        nodeTags.join("-") +
        ":" +
        Object.values(args)
            .map((v) => (v ? "1" : "0"))
            .join("");

    const fileName =
        nodeTags.length > 1 ? "2025-11-19 10-55-11 Cluster Wide.zip" : "2025-11-19 10-55-11 Single Node.zip";

    return (
        <div className="flex-window padding-xs" key={remountKey}>
            <div className="bs5 debug-package-analyzer content-margin">
                <div className="d-flex justify-content-between align-items-start gap-3 flex-wrap mb-4">
                    <div>
                        <AboutViewHeading
                            title="Debug Package Analyzer"
                            backUrl="#admin/settings/debugPackage"
                            marginBottom={1}
                        />
                        <p className="text-muted fs-5 mb-0">
                            Examine the package to identify the problem with your server
                        </p>
                    </div>
                    <PackageSummary
                        fileName={fileName}
                        issues={flattenIssues(summary)}
                        onReset={() => undefined}
                        onViewIssues={() => undefined}
                    />
                </div>
                <DebugPackageAnalysisView summary={summary} />
            </div>
        </div>
    );
}

export const SingleNode: StoryObj<SectionToggles> = {
    name: "Loaded - Single node",
    args: defaultSectionArgs,
    argTypes: sectionArgTypes,
    render: (args) => renderAnalyzer(["A"], args),
};

export const MultiNode: StoryObj<SectionToggles> = {
    name: "Loaded - Multi node (cluster)",
    args: defaultSectionArgs,
    argTypes: sectionArgTypes,
    render: (args) => renderAnalyzer(["A", "B", "C"], args),
};

function makeVerdictIssue(
    severity: Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueSeverity,
    index: number
): FlatIssue {
    return {
        key: `${severity}-${index}`,
        title: "Sample issue",
        description: "Sample description",
        recommendedAction: "",
        severity,
        category: "Server",
        scope: "node",
        nodeTags: ["A"],
    };
}

function VerdictRow({ label, issues }: { label: string; issues: FlatIssue[] }) {
    return (
        <div className="mb-4">
            <div className="text-muted mb-1">{label}</div>
            <AnalysisVerdict issues={issues} onViewIssues={() => undefined} />
        </div>
    );
}

export const HealthVerdict: StoryObj<typeof DebugPackage> = {
    name: "Health verdict - states",
    render: () => (
        <div className="bs5 debug-package-analyzer content-margin">
            <VerdictRow
                label="Error present"
                issues={[
                    makeVerdictIssue("Error", 0),
                    makeVerdictIssue("Warning", 1),
                    makeVerdictIssue("Warning", 2),
                    makeVerdictIssue("Warning", 3),
                    makeVerdictIssue("Info", 4),
                ]}
            />
            <VerdictRow label="Warnings only" issues={[makeVerdictIssue("Warning", 0), makeVerdictIssue("Info", 1)]} />
            <VerdictRow label="Info only" issues={[makeVerdictIssue("Info", 0)]} />
            <VerdictRow label="Healthy (clean package)" issues={[]} />
        </div>
    ),
};
