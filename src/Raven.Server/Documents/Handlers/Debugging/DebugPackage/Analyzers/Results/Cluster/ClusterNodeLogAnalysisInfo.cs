using System;
using System.Collections.Generic;
using Raven.Server.Rachis;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;

public class ClusterNodeLogAnalysisInfo
{
    public DebugPackageEntries.Entry LogEntry { get; set; }
    
    public LogSummary LogSummary { get; set; }
    
    public DateTime? DebugInfoCollectedAt { get; set; }
    
    public List<RaftDebugView.PeerConnection> ConnectionToPeers { get; set; }
}
