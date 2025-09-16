using Raven.Server.ServerWide.Maintenance;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;

public class ClusterObserverAnalysisInfo
{
    public DebugPackageEntries.Entry ObserverDecisionsEntry { get; set; }
    
    public ClusterObserverDecisions ObserverDecisions { get; set; }
}
