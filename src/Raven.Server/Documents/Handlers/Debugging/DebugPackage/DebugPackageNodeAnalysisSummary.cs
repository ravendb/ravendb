using NuGet.Protocol;
using Raven.Server.Dashboard;
using Raven.Server.Dashboard.Cluster.Notifications;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageNodeAnalysisSummary : IDynamicJson
{
    public ClusterOverviewPayload ClusterNodeInfo { get; set; }
    
    
    public CpuUsageAnalysisInfo CpuUsageInfo { get; set; }
    public MemoryAnalysisInfo MemoryUsageInfo { get; set; }
    // TODO arek - missing debug endpoint info public StorageUsagePayload StorageUsage { get; set; }
    public GcInfoPayload.GcMemoryInfo GcInfo { get; set; }
    
    public DatabaseOverviewPayload DatabasesOverview { get; set; }
    public DatabaseStorageUsagePayload DatabaseStorageUsage { get; set; }
    public OngoingTasksPayload DatabasesOngoingTasks { get; set; } 
    public IndexingSpeedPayload DatabaseIndexingSpeed { get; set; }
    
    public DebugPackageAnalysisIssues DetectedIssues  {  get; set; }
    public DebugPackageAnalyzeErrors AnalyzeErrors { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ClusterNodeInfo)] = ClusterNodeInfo?.ToJson(),
            [nameof(CpuUsageInfo)] = CpuUsageInfo?.ToJson(),
            [nameof(MemoryUsageInfo)] = MemoryUsageInfo?.ToJson(),
            [nameof(GcInfo)] = GcInfo?.ToJson(),
            [nameof(DatabasesOverview)] = DatabasesOverview?.ToJson(),
            [nameof(DatabaseStorageUsage)] = DatabaseStorageUsage?.ToJson(),
            [nameof(DatabasesOngoingTasks)] = DatabasesOngoingTasks?.ToJson(),
            [nameof(DatabaseIndexingSpeed)] = DatabaseIndexingSpeed?.ToJson(),
            [nameof(DetectedIssues)] = DetectedIssues?.ToJson(),
            [nameof(AnalyzeErrors)] = AnalyzeErrors?.ToJson()
        };
    }
}
