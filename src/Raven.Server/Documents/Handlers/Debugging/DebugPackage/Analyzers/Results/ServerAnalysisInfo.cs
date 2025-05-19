using System.Collections.Generic;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class ServerAnalysisInfo
{
    public BasicServerInfo BasicServerInfo { get; set; }

    public Dictionary<string, ConfigurationEntrySingleValue> ServerSettings { get; set; } = new();
    
    public NetworkAnalysisInfo NetworkInfo { get; set; }
    
    public CpuUsageAnalysisInfo CpuUsageInfo { get; set; }
    
    public MemoryAnalysisInfo MemoryInfo { get; set; }
    
    public ThreadsAnalysisInfo ThreadsInfo { get; set; }
    
    public string ServerUrl { get; set; }
    public string PublicServerUrl { get; set; }
}
