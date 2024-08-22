using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
using Raven.Server.ServerWide;

namespace Raven.Server.Utils;

internal static class OngoingTasksUtils
{
    internal static string WhoseTaskIsIt(
        ServerStore serverStore,
        DatabaseTopology databaseTopology,
        IDatabaseTask configuration,
        IDatabaseTaskStatus taskStatus,
        NotificationCenter.NotificationCenter notificationCenter,
        List<string> explanations = null)
    {
        return WhoseTaskIsIt(serverStore, databaseTopology, serverStore.Engine.CurrentState, configuration, taskStatus, notificationCenter, explanations);
    }

    internal static string WhoseTaskIsIt(ServerStore serverStore, DatabaseTopology databaseTopology, RachisState currentState, IDatabaseTask configuration, IDatabaseTaskStatus taskStatus,
        NotificationCenter.NotificationCenter notificationCenter, List<string> explanations = null)
    {
        Debug.Assert(taskStatus is not PeriodicBackupStatus);

        var whoseTaskIsIt = databaseTopology.WhoseTaskIsIt(
            currentState, configuration,
            getLastResponsibleNode:
            () =>
            {
                var lastResponsibleNode = taskStatus?.NodeTag;
                if (lastResponsibleNode == null)
                {
                    // first time this task is assigned
                    explanations?.Add("There is no last responsible node. It's first time this task is assigned");
                    return null;
                }

                if (databaseTopology.AllNodes.Contains(lastResponsibleNode) == false)
                {
                    // the topology doesn't include the last responsible node anymore
                    // we'll choose a different one
                    explanations?.Add($"The topology ({string.Join(',', databaseTopology.AllNodes)}) doesn't include the last responsible node anymore. We'll choose a different one");
                    return null;
                }

                if (serverStore.LicenseManager.HasHighlyAvailableTasks() == false)
                {
                    // can't redistribute, keep it on the original node

                    RaiseAlertIfNecessary(databaseTopology, configuration, lastResponsibleNode, serverStore, notificationCenter);

                    explanations?.Add($"Keeping the task on last responsible node ({lastResponsibleNode}). Highly available tasks aren't available");

                    return lastResponsibleNode;
                }

                explanations?.Add($"Last responsible node is {lastResponsibleNode}. Redistributing task work since highly available tasks are supported");

                return null;
            }, explanations);

        return whoseTaskIsIt;
    }

    internal static void RaiseAlertIfNecessary(DatabaseTopology databaseTopology, IDatabaseTask configuration, string lastResponsibleNode,
        ServerStore serverStore, NotificationCenter.NotificationCenter notificationCenter)
    {
        // raise alert if redistribution is necessary
        if (notificationCenter != null &&
            databaseTopology.Count > 1 &&
            serverStore.NodeTag != lastResponsibleNode &&
            databaseTopology.Members.Contains(lastResponsibleNode) == false)
        {
            var alert = LicenseManager.CreateHighlyAvailableTasksAlert(databaseTopology, configuration, lastResponsibleNode);
            notificationCenter.Add(alert);
        }
    }
}
