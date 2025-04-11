using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class ServerAnalysisInfo
{
    public BasicServerInfo BasicServerInfo { get; set; }
    
    public NetworkAnalysisInfo NetworkInfo { get; set; }

    public DatabasesOverviewAnalysisInfo DatabasesOverview { get; set; }
    
    public CpuUsageAnalysisInfo CpuUsageInfo { get; set; }
    
    public MemoryAnalysisInfo MemoryInfo { get; set; }
    
    public ThreadsAnalysisInfo ThreadsInfo { get; set; }
}
