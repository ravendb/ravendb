using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageAnalysisSummary : IDynamicJson
{
    public DebugPackageAnalysisSummary()
    {
        // deserialization
    }
    
    public DebugPackageAnalysisSummary(DebugPackageReport packageAnalysisReport)
    {
        PackageId = packageAnalysisReport.PackageId;
        SummaryPerNode = new Dictionary<string, DebugPackageNodeAnalysisSummary>(packageAnalysisReport.Reports.Length);
        
        foreach (var report in packageAnalysisReport.Reports)
        {
            var nodeTag = report.NodeTag;
            var summary = GetNodeSummary(report);
            
            SummaryPerNode.Add(nodeTag, summary);
        }

        ClusterWideIssues = packageAnalysisReport.ClusterWideIssues;
    }
    public string PackageId { get; set; }
    public Dictionary<string, DebugPackageNodeAnalysisSummary> SummaryPerNode { get; set; }
    public DebugPackageAnalysisIssues ClusterWideIssues { get; set; }

    private DebugPackageNodeAnalysisSummary GetNodeSummary(DebugPackageNodeReport nodeReport)
    {
        return new DebugPackageNodeAnalysisSummary
        {
            BasicServerInfo = nodeReport.Server.BasicServerInfo,
            DatabasesOverview = nodeReport.Server.DatabasesOverview,
            MachineInfo = nodeReport.Machine,
            BasicMemoryInfo = nodeReport.Server.MemoryInfo.GetBasicInfo(),
            DetectedIssues = nodeReport.DetectedIssues,
            AnalyzeErrors = nodeReport.AnalyzeErrors,
        };
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(PackageId)] = PackageId,
            [nameof(SummaryPerNode)] = SummaryPerNode != null 
                ? DynamicJsonValue.Convert(SummaryPerNode)
                : null,
            [nameof(ClusterWideIssues)] = ClusterWideIssues?.ToJson()
        };
    }
}
