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
    nodeTag?: string;
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

export function flattenIssues(summary: DebugPackageAnalysisSummary): FlatIssue[] {
    const result: FlatIssue[] = [];
    let counter = 0;

    const push = (issue: DetectedIssue, scope: IssueScope, nodeTag?: string, database?: string) => {
        if (!issue) {
            return;
        }
        result.push({
            key: `${scope}-${nodeTag ?? ""}-${database ?? ""}-${counter++}`,
            title: issue.Title,
            description: issue.Description,
            recommendedAction: issue.RecommendedAction,
            severity: issue.Severity,
            category: issue.Category,
            scope,
            nodeTag,
            database,
        });
    };

    const addNonDatabaseBuckets = (issues: DebugPackageAnalysisIssues, scope: IssueScope, nodeTag?: string) => {
        if (!issues) {
            return;
        }
        (issues.ServerIssues ?? []).forEach((x) => push(x, scope, nodeTag));
        (issues.ClusterIssues ?? []).forEach((x) => push(x, scope, nodeTag));
        Object.entries(issues.DatabaseIssues ?? {}).forEach(([database, list]) =>
            (list ?? []).forEach((x) => push(x, "database", nodeTag, database))
        );
    };

    // cluster-wide findings (cross-node comparisons, observer, etc.)
    addNonDatabaseBuckets(summary.ClusterWideIssues, "cluster-wide");

    // per-node findings
    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        addNonDatabaseBuckets(node.DetectedIssues, "node", nodeTag);
    });

    return result;
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
