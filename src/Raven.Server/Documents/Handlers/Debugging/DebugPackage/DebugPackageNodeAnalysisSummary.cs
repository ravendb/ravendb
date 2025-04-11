using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageNodeAnalysisSummary : IDynamicJson
{
    public BasicServerInfo BasicServerInfo { get; set; }
    
    public DatabasesOverviewAnalysisInfo DatabasesOverview { get; set; }

    public MachineAnalysisInfo MachineInfo { get; set; }
    
    public MemoryAnalysisBasicInfo BasicMemoryInfo { get; set; }
    
    public DebugPackageAnalysisIssues DetectedIssues  {  get; set; }
    
    public DebugPackageAnalyzeErrors AnalyzeErrors { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(MachineInfo)] = MachineInfo?.ToJson(),
            [nameof(BasicServerInfo)] = BasicServerInfo?.ToJson(),
            [nameof(DatabasesOverview)] = DatabasesOverview?.ToJson(),
            [nameof(BasicMemoryInfo)] = BasicMemoryInfo?.ToJson(),
            [nameof(DetectedIssues)] = DetectedIssues?.ToJson(),
            [nameof(AnalyzeErrors)] = AnalyzeErrors?.ToJson()
        };
    }
}
