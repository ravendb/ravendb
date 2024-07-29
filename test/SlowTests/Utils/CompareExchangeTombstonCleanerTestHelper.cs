using System.Collections.Generic;
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
    public static async Task<ClusterObserver.CompareExchangeTombstonesCleanupState> Clean(ClusterOperationContext context, string database, RavenServer server, bool ignoreClustrTrx)
    {
        CleanCompareExchangeTombstonesCommand cmd;
        var serverStore = server.ServerStore;
        using (var rawRecord = serverStore.Cluster.ReadRawDatabaseRecord(context, database))
        {
            var current = serverStore.Observer.Maintenance.GetStats();
            var previous = serverStore.Observer.Maintenance.GetStats();

            var mergedState = new ClusterObserver.MergedDatabaseObservationState(rawRecord);
            if (rawRecord.IsSharded)
            {
                foreach ((var name, var topology) in rawRecord.Topologies)
                {
                    AddState(name, rawRecord, topology, current, previous, mergedState);
                }
            }
            else
            {
                AddState(database, rawRecord, rawRecord.Topology, current, previous, mergedState);
            }
            
            cmd = serverStore.Observer.GetCompareExchangeTombstonesToCleanup(database, mergedState, context, out var cleanupState);
            if (cleanupState != ClusterObserver.CompareExchangeTombstonesCleanupState.HasMoreTombstones)
                return cleanupState;

            Assert.NotNull(cmd);
        }

        var result = await serverStore.SendToLeaderAsync(cmd);
        await serverStore.Cluster.WaitForIndexNotification(result.Index);

        var hasMore = (bool)result.Result;
        return hasMore
            ? ClusterObserver.CompareExchangeTombstonesCleanupState.HasMoreTombstones
            : ClusterObserver.CompareExchangeTombstonesCleanupState.NoMoreTombstones;

        void AddState(string name, RawDatabaseRecord rawRecord, DatabaseTopology topology, Dictionary<string, ClusterNodeStatusReport> current, Dictionary<string, ClusterNodeStatusReport> previous, ClusterObserver.MergedDatabaseObservationState mergedState)
        {
            var state = new ClusterObserver.DatabaseObservationState(name, rawRecord, topology, serverStore.GetClusterTopology(context), current, previous, 0,
                0);
            if (ignoreClustrTrx)
            {
                foreach ((var key, var value) in state.Current)
                {
                    foreach ( (var inKey, var inValue) in value.Report)
                    {
                        inValue.LastClusterWideTransactionRaftIndex = long.MaxValue;
                    }
                }
            }
            mergedState.AddState(state);
        }
    }
}
