using System.Collections.Generic;
using Raven.Client.Http;
using Raven.Client.ServerWide;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;

public class ClusterNodeStateAnalysisInfo
{
    public DebugPackageEntries.Entry TopologyEntry { get; set; }
    public ClusterTopologyResponse Topology { get; set; }
    public RachisState? CurrentState { get; set; }
    public string LastStateChangeReason { get; set; }
    public long? CurrentTerm { get; set; }
    public List<ClusterAlert> ClusterTopologyWarningAlerts { get; set; }
}
