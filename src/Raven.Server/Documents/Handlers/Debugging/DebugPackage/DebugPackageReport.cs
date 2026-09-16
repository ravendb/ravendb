using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageReport
{
    public readonly string PackageId = Guid.NewGuid().ToString("N").Substring(0, 10);
    private DebugPackageAnalysisSummary _summary;

    public DebugPackageReport(DebugPackageNodeReport[] nodeReports, Dictionary<string, string> unreachableNodes, DebugPackageAnalysisIssues clusterWideIssues)
    {
        Reports = nodeReports;
        UnreachableNodes = unreachableNodes;
        ClusterWideIssues = clusterWideIssues;
    }

    public DateTime LastAccessTime { get; set; }
    public DebugPackageAnalysisIssues ClusterWideIssues { get; set; }
    public DebugPackageNodeReport[] Reports { get; }
    public Dictionary<string, string> UnreachableNodes { get; }

    public DebugPackageAnalysisSummary GetSummary()
    {
        return _summary ??= new DebugPackageAnalysisSummary(this);
    }

    public DebugPackageNodeReport ForNode(string nodeTag)
    {
        return Reports.SingleOrDefault(x => x.NodeTag == nodeTag) ?? throw new InvalidOperationException($"There is no report for node '{nodeTag}'");
    }
}
