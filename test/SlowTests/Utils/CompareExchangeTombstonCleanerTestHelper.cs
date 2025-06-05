using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Xunit;

namespace SlowTests.Utils;

internal static class CompareExchangeTombstoneCleanerTestHelper
{
    public static async Task<ClusterObserver.CompareExchangeTombstonesCleanupState> Clean(ClusterContextPool contextPool, string database, RavenServer server, bool ignoreClustrTrx, StringBuilder sb = null)
    {
        sb ??= new StringBuilder();
        CleanCompareExchangeTombstonesCommand cmd;
        var serverStore = server.ServerStore;

        using (contextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        using (var rawRecord = serverStore.Cluster.ReadRawDatabaseRecord(context, database))
        {
            var current = serverStore.Observer.Maintenance.GetStats();
            var previous = serverStore.Observer.Maintenance.GetStats();

            var mergedState = new ClusterObserver.MergedDatabaseObservationState(rawRecord);
            if (rawRecord.IsSharded)
            {
                foreach ((string name, DatabaseTopology topology) in rawRecord.Topologies)
                    AddState(name, topology);
            }
            else
            {
                AddState(database, rawRecord.Topology);
            }

            var beforeCleanupCompareExchangeTombstonesNumber = serverStore.Cluster.GetNumberOfCompareExchangeTombstones(context, database);
            sb.AppendLine($"Before cleanup: {beforeCleanupCompareExchangeTombstonesNumber} tombstones.");

            cmd = serverStore.Observer.GetCompareExchangeTombstonesToCleanup(database, mergedState, context, out var cleanupState);
            if (cleanupState != ClusterObserver.CompareExchangeTombstonesCleanupState.HasMoreTombstones)
            {
                sb.AppendLine($"Exiting early, cleanupState: {cleanupState}");
                return cleanupState;
            }

            void AddState(string name, DatabaseTopology topology)
            {
                var state = new ClusterObserver.DatabaseObservationState(name, rawRecord, topology, server.ServerStore.GetClusterTopology(context), current, previous, lastIndexModification: 0, observerIteration: 0);
                if (ignoreClustrTrx)
                {
                    foreach ((string _, var clusterNodeStatusReport) in state.Current)
                        foreach ((string _, var databaseStatusReport) in clusterNodeStatusReport.Report)
                            databaseStatusReport.LastClusterWideTransactionRaftIndex = long.MaxValue;
                }
                mergedState.AddState(state);
            }
        }

        Assert.NotNull(cmd);

        sb.AppendLine($"Sending {nameof(CleanCompareExchangeTombstonesCommand)} with values: Database `{cmd.DatabaseName}`, MaxRaftIndex `{cmd.MaxRaftIndex}, Take `{cmd.Take}`");
        var result = await serverStore.SendToLeaderAsync(cmd);
        await serverStore.Cluster.WaitForIndexNotification(result.Index);

        var hasMore = (bool)result.Result;

        using (contextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            long afterCleanupCompareExchangeTombstonesNumber = serverStore.Cluster.GetNumberOfCompareExchangeTombstones(context, database);
            sb.AppendLine($"After cleanup: {afterCleanupCompareExchangeTombstonesNumber} tombstones.");
        }

        return hasMore
            ? ClusterObserver.CompareExchangeTombstonesCleanupState.HasMoreTombstones
            : ClusterObserver.CompareExchangeTombstonesCleanupState.NoMoreTombstones;
    }
}
