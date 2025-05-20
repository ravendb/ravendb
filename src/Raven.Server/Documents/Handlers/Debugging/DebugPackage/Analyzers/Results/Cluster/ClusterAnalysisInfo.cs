using System;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;

public class ClusterAnalysisInfo
{
    public ClusterNodeStateAnalysisInfo NodeStateInfo { get; set; }
    
    public ClusterNodeLogAnalysisInfo NodeLogInfo { get; set; }
    
    public ClusterObserverAnalysisInfo ObserverInfo { get; set; }
    
    public long? ElectionTimeoutInMs { get; set; }
    
    public long? DefaultElectionTimeoutInMs { get; set; }

}
