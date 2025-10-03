using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Rachis;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;

public class ClusterNodeLogAnalysisInfo
{
    public DebugPackageEntries.Entry LogEntry { get; set; }
    
    public LogSummary LogSummary { get; set; }
    
    public DateTime? DebugInfoCollectedAt { get; set; }
    
    public List<RaftDebugView.PeerConnection> ConnectionToPeers { get; set; }
    
    public long GetQueueSize()
    {
        if (LogSummary.Logs.Any() == false)
            return 0;

        return LogSummary.LastLogEntryIndex - LogSummary.CommitIndex;
    }
    
    public int GetProgress()
    {
        var first = LogSummary.FirstEntryIndex;
        var last = LogSummary.LastLogEntryIndex;

        var logLength = last - first + 1;
        var queueLength = GetQueueSize();

        return (int)Math.Ceiling(100d * (logLength - queueLength) / logLength);
    }
}
