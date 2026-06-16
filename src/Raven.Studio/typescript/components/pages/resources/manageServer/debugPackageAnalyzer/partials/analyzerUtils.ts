import IconName from "typings/server/icons";
import genUtils from "common/generalUtils";

type OSType = Raven.Client.ServerWide.Operations.OSType;
type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type DebugPackageAnalysisIssues =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.DebugPackageAnalysisIssues;
type DetectedIssue = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.DetectedIssue;
type IssueSeverity = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueSeverity;
type IssueCategory = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueCategory;

export type IssueScope = "cluster-wide" | "node" | "database";

export interface FlatIssue {
    key: string;
    title: string;
    description: string;
    recommendedAction: string;
    severity: IssueSeverity;
    category: IssueCategory;
    scope: IssueScope;
    // every node that reported this (identical) finding; empty for cluster-wide issues
    nodeTags: string[];
    database?: string;
}

export const severityOrder: IssueSeverity[] = ["Error", "Warning", "Info", "None"];

export const issueCategories: IssueCategory[] = ["General", "Cluster", "Server", "Database", "Indexes"];

export const issueScopes: IssueScope[] = ["cluster-wide", "node", "database"];

export function scopeLabel(scope: IssueScope): string {
    switch (scope) {
        case "cluster-wide":
            return "Cluster-Wide";
        case "node":
            return "Node";
        case "database":
            return "Database";
        default:
            return scope;
    }
}

// The server analyzes each node independently, so a database- or cluster-wide property (encryption
// enabled, an index defined to use Corax, etc.) is reported once per node as an identical issue.
// We collapse issues with identical content into a single item that carries every contributing node
// tag. Generic and content-driven - no per-issue-type logic; only the node tag is allowed to differ.
export function flattenIssues(summary: DebugPackageAnalysisSummary): FlatIssue[] {
    const byKey = new Map<string, FlatIssue>();

    const add = (issue: DetectedIssue, scope: IssueScope, nodeTag?: string, database?: string) => {
        if (!issue) {
            return;
        }

        const key = [
            scope,
            issue.Category,
            issue.Severity,
            database ?? "",
            issue.Title ?? "",
            issue.Description ?? "",
            issue.RecommendedAction ?? "",
        ].join(" ");

        const existing = byKey.get(key);
        if (existing) {
            if (nodeTag && existing.nodeTags.includes(nodeTag) === false) {
                existing.nodeTags.push(nodeTag);
            }
            return;
        }

        byKey.set(key, {
            key,
            title: issue.Title,
            description: issue.Description,
            recommendedAction: issue.RecommendedAction,
            severity: issue.Severity,
            category: issue.Category,
            scope,
            nodeTags: nodeTag ? [nodeTag] : [],
            database,
        });
    };

    const addBuckets = (issues: DebugPackageAnalysisIssues, scope: IssueScope, nodeTag?: string) => {
        if (!issues) {
            return;
        }
        (issues.ServerIssues ?? []).forEach((x) => add(x, scope, nodeTag));
        (issues.ClusterIssues ?? []).forEach((x) => add(x, scope, nodeTag));
        Object.entries(issues.DatabaseIssues ?? {}).forEach(([database, list]) =>
            (list ?? []).forEach((x) => add(x, "database", nodeTag, database))
        );
    };

    // cluster-wide findings (cross-node comparisons, observer, etc.)
    addBuckets(summary.ClusterWideIssues, "cluster-wide");

    // per-node findings - identical ones across nodes merge into the items created above
    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        addBuckets(node.DetectedIssues, "node", nodeTag);
    });

    const result = Array.from(byKey.values());
    result.forEach((issue) => issue.nodeTags.sort());
    return result;
}

export function formatPercentage(value: number | undefined): string {
    if (value == null) {
        return "-";
    }
    return `${Math.round(value)}%`;
}

export function formatNumber(value: number | undefined): string {
    if (value == null) {
        return "-";
    }
    return value.toLocaleString();
}

export function countBy<T extends string>(
    issues: FlatIssue[],
    selector: (issue: FlatIssue) => T
): Record<string, number> {
    const counts: Record<string, number> = {};
    for (const issue of issues) {
        const key = selector(issue);
        counts[key] = (counts[key] ?? 0) + 1;
    }
    return counts;
}

export function osIcon(osType: OSType): IconName {
    switch (osType) {
        case "Linux":
            return "linux";
        case "Windows":
            return "windows";
        case "MacOS":
            return "apple";
        default:
            return "server";
    }
}

// UpTime arrives as a serialized .NET TimeSpan (e.g. "45.12:33:00.123") which moment.duration parses directly
export function parseUpTimeSeconds(upTime: string): number {
    return genUtils.timeSpanToSeconds(upTime) ?? -1;
}

export function formatUpTime(upTime: string): string {
    if (!upTime) {
        return "-";
    }
    return genUtils.formatTimeSpan(upTime, true);
}
