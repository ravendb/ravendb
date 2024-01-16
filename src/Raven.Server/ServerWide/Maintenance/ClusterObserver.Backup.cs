using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.ServerWide.Maintenance;

internal partial class ClusterObserver
{
    private readonly Dictionary<long, ResponsibleNodeForBackup.ChosenNodeReason> _lastChosenNodeReasonPerTask = new();

    private List<ResponsibleNodeInfo> GetResponsibleNodesForBackupTasks(Leader currentLeader, RawDatabaseRecord rawRecord, string databaseName, DatabaseTopology topology, TransactionOperationContext context)
    {
        // we must verify that all connected nodes are supporting the UpdateResponsibleNodeForTasksCommand command
        foreach (var version in currentLeader.PeersVersion.Values)
        {
            if (version < UpdateResponsibleNodeForTasksCommand.CommandVersion)
            {
                // the command isn't supported
                return null;
            }
        }

        var moveToNewResponsibleNodeGracePeriodConfig = (TimeSetting)RavenConfiguration.GetValue(x => x.Backup.MoveToNewResponsibleNodeGracePeriod, _server.Configuration, rawRecord.Settings);
        var moveToNewResponsibleNodeGracePeriod = moveToNewResponsibleNodeGracePeriodConfig.AsTimeSpan;
        List<ResponsibleNodeInfo> responsibleNodeCommands = null;

        foreach (var configuration in rawRecord.PeriodicBackups)
        {
            var responsibleNodeInfo = GetResponsibleNodeInfo(databaseName, configuration, topology, moveToNewResponsibleNodeGracePeriod, context);
            if (responsibleNodeInfo == null)
                continue;

            responsibleNodeCommands ??= new List<ResponsibleNodeInfo>();
            responsibleNodeCommands.Add(responsibleNodeInfo);
        }

        return responsibleNodeCommands;
    }

    private ResponsibleNodeInfo GetResponsibleNodeInfo(
            string databaseName,
            PeriodicBackupConfiguration configuration,
            DatabaseTopology topology,
            TimeSpan moveToNewResponsibleNodeGracePeriod,
            TransactionOperationContext context)
    {
        var responsibleNodeBlittable = BackupUtils.GetResponsibleNodeInfoFromCluster(_server, context, databaseName, configuration.TaskId);
        string currentResponsibleNode = null;
        responsibleNodeBlittable?.TryGet(nameof(ResponsibleNodeInfo.ResponsibleNode), out currentResponsibleNode);

        var newResponsibleNode = GetResponsibleNodeForBackup(databaseName, configuration, topology, moveToNewResponsibleNodeGracePeriod, context, currentResponsibleNode);
        if (newResponsibleNode == null)
        {
            // didn't find a suitable node for backup
            return null;
        }

        DateTime? notSuitableForTaskSince = null;
        responsibleNodeBlittable?.TryGet(nameof(ResponsibleNodeInfo.NotSuitableForTaskSince), out notSuitableForTaskSince);

        switch (newResponsibleNode.Reason)
        {
            case ResponsibleNodeForBackup.ChosenNodeReason.SameResponsibleNode:
            case ResponsibleNodeForBackup.ChosenNodeReason.SameResponsibleNodeDueToResourceLimitations:
            case ResponsibleNodeForBackup.ChosenNodeReason.SameResponsibleNodeDueToMissingHighlyAvailableTasks:
                if (currentResponsibleNode == null)
                {
                    // backward compatibility - missing responsible node in the cluster storage
                    return new ResponsibleNodeInfo
                    {
                        TaskId = configuration.TaskId,
                        ResponsibleNode = newResponsibleNode.NodeTag
                    };
                }

                if (notSuitableForTaskSince != null)
                {
                    // we need to remove the NotSuitableForTaskSince since the node is suitable for backup

                    AddToDecisionLog(databaseName, $"Node '{currentResponsibleNode}' was in rehab for {DateTime.UtcNow - notSuitableForTaskSince}. " +
                                                   $"Since it's now in a member state, the backup task '{configuration.Name}' will continue to run on that node");

                    return new ResponsibleNodeInfo
                    {
                        TaskId = configuration.TaskId,
                        ResponsibleNode = newResponsibleNode.NodeTag
                    };
                }

                AddToDecisionLog(newResponsibleNode, configuration.TaskId, databaseName);

                // it's the same responsible node for backup, noop
                return null;

            case ResponsibleNodeForBackup.ChosenNodeReason.MentorNode:
            case ResponsibleNodeForBackup.ChosenNodeReason.PinnedMentorNode:
            case ResponsibleNodeForBackup.ChosenNodeReason.NonExistingResponsibleNode:
            case ResponsibleNodeForBackup.ChosenNodeReason.CurrentResponsibleNodeRemovedFromTopology:
                AddToDecisionLog(newResponsibleNode, configuration.TaskId, databaseName);
                return new ResponsibleNodeInfo
                {
                    TaskId = configuration.TaskId,
                    ResponsibleNode = newResponsibleNode.NodeTag
                };

            case ResponsibleNodeForBackup.ChosenNodeReason.CurrentResponsibleNodeNotResponding:
                if (notSuitableForTaskSince == null)
                {
                    // it's the first time that we identify that the node isn't suitable for backup
                    AddToDecisionLog(databaseName, $"Node '{currentResponsibleNode}' is currently in rehab state. " +
                                                   $"The backup task '{configuration.Name}' will be moved to another node at: {DateTime.UtcNow + moveToNewResponsibleNodeGracePeriod} (UTC)");

                    return new ResponsibleNodeInfo
                    {
                        TaskId = configuration.TaskId,
                        ResponsibleNode = currentResponsibleNode,
                        NotSuitableForTaskSince = DateTime.UtcNow
                    };
                }

                if (DateTime.UtcNow - notSuitableForTaskSince.Value < moveToNewResponsibleNodeGracePeriod)
                {
                    // grace period before moving the task to another node
                    return null;
                }

                AddToDecisionLog(newResponsibleNode, configuration.TaskId, databaseName);
                return new ResponsibleNodeInfo
                {
                    TaskId = configuration.TaskId,
                    ResponsibleNode = newResponsibleNode.NodeTag
                };

            default:
                throw new ArgumentOutOfRangeException($"{nameof(newResponsibleNode.Reason)}", $"Unknown chosen node reason {newResponsibleNode.Reason}");
        }
    }

    private ResponsibleNodeForBackup GetResponsibleNodeForBackup(
        string databaseName,
        PeriodicBackupConfiguration configuration,
        DatabaseTopology topology,
        TimeSpan moveToNewResponsibleNodeGracePeriod,
        TransactionOperationContext context,
        string currentResponsibleNode)
    {
        var mentorNode = configuration.GetMentorNode();
        if (mentorNode != null)
        {
            if (topology.Members.Contains(mentorNode))
            {
                return new MentorNode(mentorNode, configuration, _lastChosenNodeReasonPerTask);
            }

            if (topology.AllNodes.Contains(mentorNode) && configuration.IsPinnedToMentorNode())
            {
                return new PinnedMentorNode(mentorNode, configuration, _lastChosenNodeReasonPerTask);
            }
        }

        var lastResponsibleNode = currentResponsibleNode ??
                                  // backward compatibility - will continue running the backup on the last node that ran the backup
                                  BackupUtils.GetBackupStatusFromCluster(_server, context, databaseName, configuration.TaskId)?.NodeTag;

        if (lastResponsibleNode == null)
        {
            // we don't have a responsible node for the backup
            var newNode = topology.WhoseTaskIsIt(configuration);
            return new NonExistingResponsibleNode(newNode, configuration.Name);
        }

        if (topology.AllNodes.Contains(lastResponsibleNode) == false)
        {
            // the responsible node for the backup is not in the topology anymore
            var newNode = topology.WhoseTaskIsIt(configuration);
            return new CurrentResponsibleNodeRemovedFromTopology(newNode, configuration.Name, lastResponsibleNode);
        }

        if (topology.Rehabs.Contains(lastResponsibleNode) &&
            topology.PromotablesStatus.TryGetValue(lastResponsibleNode, out var status) &&
            (status == DatabasePromotionStatus.OutOfCpuCredits ||
             status == DatabasePromotionStatus.EarlyOutOfMemory ||
             status == DatabasePromotionStatus.HighDirtyMemory))
        {
            // avoid moving backup tasks when the machine is out of CPU credits or out of memory
            return new SameResponsibleNodeDueToResourceLimitations(lastResponsibleNode, configuration, _lastChosenNodeReasonPerTask, status);
        }

        if (_server.LicenseManager.HasHighlyAvailableTasks() == false)
        {
            // can't redistribute, keep it on the original node
            BackupUtils.RaiseAlertIfNecessary(topology, configuration, lastResponsibleNode, _server, _server.NotificationCenter);
            return new SameResponsibleNodeDueToMissingHighlyAvailableTasks(lastResponsibleNode, configuration, _lastChosenNodeReasonPerTask);
        }

        if (topology.Members.Contains(lastResponsibleNode))
        {
            // this is the same node that we had before
            return new SameResponsibleNode(lastResponsibleNode, configuration.Name);
        }

        // find a new responsible node
        var newResponsibleNode = topology.WhoseTaskIsIt(configuration);
        if (newResponsibleNode == null)
            return null;

        return new CurrentResponsibleNodeNotResponding(newResponsibleNode, configuration.Name,lastResponsibleNode, moveToNewResponsibleNodeGracePeriod);
    }

    private void AddToDecisionLog(ResponsibleNodeForBackup nodeForBackup, long taskId, string database)
    {
        if (nodeForBackup.ShouldLog == false)
            return;

        _lastChosenNodeReasonPerTask[taskId] = nodeForBackup.Reason;

        Debug.Assert(nodeForBackup.ReasonForDecisionLog != null);
        AddToDecisionLog(database, nodeForBackup.ReasonForDecisionLog);
    }
}
