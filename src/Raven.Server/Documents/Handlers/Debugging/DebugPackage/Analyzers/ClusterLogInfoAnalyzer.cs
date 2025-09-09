using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;
using Raven.Server.Rachis;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class ClusterLogInfoAnalyzer : AbstractDebugPackageAnalyzer
{
    public ClusterLogInfoAnalyzer(DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : base(errors, issues)
    {
    }

    public ClusterNodeLogAnalysisInfo ClusterNodeLogInfo { get; private set; }

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        if (serverEntries.TryGetEntry<RachisAdminHandler>(x => x.GetLogs(), out var logEntry) == false)
        {
            AddWarning("Could not retrieve cluster log");
            return false;
        }

        if (logEntry.TryGetJsonValue<LogSummary>("Log", out var logSummary) == false)
        {
            AddWarning("Could not retrieve cluster log");
            return false;
        }

        ClusterNodeLogInfo = new ClusterNodeLogAnalysisInfo { LogEntry = logEntry, LogSummary = logSummary };

        if (logEntry.TryGetJson(Raven.Client.Constants.Documents.Metadata.Key, out JsonElement metadata) && metadata.TryGetProperty("DateTime", out var dateTimeField) &&
            dateTimeField.TryGetDateTime(out var retrievalInfoDateTime))
        {
            ClusterNodeLogInfo.DebugInfoCollectedAt = retrievalInfoDateTime;
        }


        if (serverEntries.TryGetValue<RachisAdminHandler, List<RaftDebugView.PeerConnection>>(x => x.GetLogs(),
                nameof(LeaderDebugView.ConnectionToPeers), out var connectionToPeers))
        {
            ClusterNodeLogInfo.ConnectionToPeers = connectionToPeers;
        }

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        var clusterLogQueueSize = ClusterNodeLogInfo.GetQueueSize();

        if (ClusterNodeLogInfo.DebugInfoCollectedAt != null)
        {
            var lastCommitAgo = ClusterNodeLogInfo.DebugInfoCollectedAt - ClusterNodeLogInfo.LogSummary.LastCommitedTime;

            if (clusterLogQueueSize >= 5 && lastCommitAgo > TimeSpan.FromMinutes(2))
            {
                issues.ClusterIssues.Add(new DetectedIssue("Cluster Log is stalled on this node",
                    $"There was no commit for {lastCommitAgo:g}, while there are {clusterLogQueueSize} pending log entries to process",
                    IssueSeverity.Error, IssueCategory.Cluster));
            }
        }

        if (clusterLogQueueSize > 128)
        {
            issues.ClusterIssues.Add(new DetectedIssue("Big number of uncommitted Cluster Log entries",
                $"There are {clusterLogQueueSize} Raft commands left to be committed",
                IssueSeverity.Warning, IssueCategory.Cluster));
        }

        var progress = ClusterNodeLogInfo.GetProgress();

        if (progress < 95)
        {
            issues.ClusterIssues.Add(new DetectedIssue("There are has pending Raft commands to commit on this node",
                $"This node has committed {progress}% of the Raft commands in the Cluster Log",
                progress < 70 ? IssueSeverity.Warning : IssueSeverity.Info, IssueCategory.Cluster));
        }

        if (ClusterNodeLogInfo.ConnectionToPeers != null)
        {
            foreach (var connection in ClusterNodeLogInfo.ConnectionToPeers)
            {
                if (connection.Connected == false)
                {
                    issues.ClusterIssues.Add(new DetectedIssue("Cluster node connectivity issue",
                        $"Current node is not connected to {connection.Destination} (status: {connection.Status})",
                        IssueSeverity.Error, IssueCategory.Cluster));
                }
            }
        }

        if (ClusterNodeLogInfo.LogSummary.CriticalError != null)
        {
            issues.ClusterIssues.Add(new DetectedIssue("Critical error in Cluster Log",
                $"This may indicate a critical operational issue in the cluster: {ClusterNodeLogInfo.LogSummary.CriticalError}",
                IssueSeverity.Error, IssueCategory.Cluster));
        }
    }
}
