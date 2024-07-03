using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;

namespace Raven.Server.Utils;

internal static class OngoingTasksUtils
{
    internal static string WhoseTaskIsIt(
        ServerStore serverStore,
        DatabaseTopology databaseTopology,
        IDatabaseTask configuration,
        IDatabaseTaskStatus taskStatus,
        AbstractNotificationCenter notificationCenter)
    {
        Debug.Assert(taskStatus is not PeriodicBackupStatus);

        var whoseTaskIsIt = databaseTopology.WhoseTaskIsIt(
            serverStore.Engine.CurrentCommittedState.State, configuration,
            getLastResponsibleNode:
            () =>
            {
                var lastResponsibleNode = taskStatus?.NodeTag;
                if (lastResponsibleNode == null)
                {
                    // first time this task is assigned
                    return null;
                }

                if (databaseTopology.AllNodes.Contains(lastResponsibleNode) == false)
                {
                    // the topology doesn't include the last responsible node anymore
                    // we'll choose a different one
                    return null;
                }

                if (serverStore.LicenseManager.HasHighlyAvailableTasks() == false)
                {
                    // can't redistribute, keep it on the original node
                    RaiseAlertIfNecessary(databaseTopology, configuration, lastResponsibleNode, serverStore, notificationCenter);
                    return lastResponsibleNode;
                }

                return null;
            });

        return whoseTaskIsIt;
    }

    internal static void RaiseAlertIfNecessary(DatabaseTopology databaseTopology, IDatabaseTask configuration, string lastResponsibleNode,
        ServerStore serverStore, AbstractNotificationCenter notificationCenter)
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
