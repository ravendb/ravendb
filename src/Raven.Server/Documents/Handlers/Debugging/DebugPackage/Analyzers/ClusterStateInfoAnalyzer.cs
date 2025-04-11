using System;
using System.Collections.Generic;
using System.Text.Json;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class ClusterStateInfoAnalyzer(DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : AbstractDebugPackageAnalyzer(errors, issues)
{
    public ClusterNodeStateAnalysisInfo ClusterNodeStateInfo { get; private set; }
    
    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        if (serverEntries.TryGetEntry<RachisAdminHandler>(x => x.GetClusterTopology(), out var topologyEntry) == false)
        {
            AddWarning("Could not retrieve cluster topology");
            return false;
        }
        
        if (serverEntries.TryGetValue<RachisAdminHandler, ClusterTopologyResponse>(x => x.GetClusterTopology(),
                out var topology) == false)
        {
            AddWarning("Could not retrieve cluster topology");
            return false;
        }
        
        ClusterNodeStateInfo = new ClusterNodeStateAnalysisInfo
        {
            Topology = topology,
            TopologyEntry = topologyEntry,
        };

        if (serverEntries.TryGetValue<RachisAdminHandler, RachisState?>(x => x.GetClusterTopology(),
                nameof(RachisConsensus.CurrentState), out var currentState))
        {
            ClusterNodeStateInfo.CurrentState = currentState;
        }
        
        if (serverEntries.TryGetValue<RachisAdminHandler, long?>(x => x.GetClusterTopology(),
                nameof(ServerStore.Engine.CurrentTerm), out var currentTerm))
        {
            ClusterNodeStateInfo.CurrentTerm = currentTerm;
        }
        
        if (serverEntries.TryGetValue<RachisAdminHandler, string>(x => x.GetClusterTopology(),
                nameof(ServerStore.Engine.LastStateChangeReason), out var lastStateChangeReason))
        {
            ClusterNodeStateInfo.LastStateChangeReason = lastStateChangeReason;
        }
        
        if (serverEntries.TryGetEntry<RachisAdminHandler>(x => x.GetClusterTopology(), out var entry))
        {
            if (entry.TryGetJson("Errors", out var clusterErrorsPerNode) && clusterErrorsPerNode.ValueKind == JsonValueKind.Array)
            {
                for (int i = 0; i < clusterErrorsPerNode.GetArrayLength(); i++)
                {
                    var nodeError = clusterErrorsPerNode[i].Deserialize<NodeError>();
                    
                    var error = new ClusterAlert
                    {
                        NodeTag = nodeError.Node,
                    };
                    
                    if (nodeError.Error.TryGetProperty(nameof(AlertRaised.Title), out var title))
                        error.Title = title.GetString();
                    
                    if (nodeError.Error.TryGetProperty(nameof(AlertRaised.Message), out var message))
                        error.Message = message.GetString();

                    if (nodeError.Error.TryGetProperty(nameof(AlertRaised.CreatedAt), out var createdAt) &&
                        DateTime.TryParse(createdAt.GetString(), out var dateTime))
                    {
                        error.CreatedAt = dateTime;
                    }
                    
                    if (nodeError.Error.TryGetProperty(nameof(AlertRaised.Details), out var details) &&
                        details.TryGetProperty(nameof(ExceptionDetails.Exception), out var exception))
                    {
                        error.Exception = exception.GetString();
                    }

                    ClusterNodeStateInfo.ClusterTopologyWarningAlerts ??= new List<ClusterAlert>();
                    ClusterNodeStateInfo.ClusterTopologyWarningAlerts.Add(error);
                }
            }
        }
        
        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        if (ClusterNodeStateInfo.Topology.ServerRole == ServerNode.Role.Rehab)
        {
            issues.ClusterIssues.Add(new DetectedIssue("Node is in Rehab state",
                "The cluster node is currently in Rehab state",
                IssueSeverity.Warning, IssueCategory.Cluster));
        }
        else if (ClusterNodeStateInfo.Topology.ServerRole == ServerNode.Role.Promotable)
        {
            issues.ClusterIssues.Add(new DetectedIssue("Node is in Promotable state",
                $"The cluster node is catching up",
                IssueSeverity.Info, IssueCategory.Cluster));
        }
        
        if (ClusterNodeStateInfo.ClusterTopologyWarningAlerts != null)
        {
            foreach (var alert in ClusterNodeStateInfo.ClusterTopologyWarningAlerts)
            {
                issues.ClusterIssues.Add(new DetectedIssue("Cluster topology warning alert",
                    $"The alert has been raised on {alert.CreatedAt:g}: {alert.Title} - {alert.Message}. Exception: {alert.Exception}",
                    IssueSeverity.Warning, IssueCategory.Cluster));
            }
        }
    }

    private class NodeError
    {
        public string Node { get; set; }
        public JsonElement Error { get; set; }
    }
}
