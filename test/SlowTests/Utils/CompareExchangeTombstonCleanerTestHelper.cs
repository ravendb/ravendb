using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Xunit;

namespace SlowTests.Utils;

internal static class CompareExchangeTombstoneCleanerTestHelper
{
    public static async Task<ClusterObserver.CompareExchangeTombstonesCleanupState> Clean(RavenServer server, string database, TransactionOperationContext context)
    {
        CleanCompareExchangeTombstonesCommand cmd;
        var serverStore = server.ServerStore;
        using (var rawRecord = serverStore.Cluster.ReadRawDatabaseRecord(context, database))
        {
            var report = new Dictionary<string, DatabaseStatusReport>
            {
                { database, new DatabaseStatusReport { Name = database, LastClusterWideTransactionRaftIndex = long.MaxValue } }
            };
            var current = rawRecord.Topology.AllNodes.ToDictionary(x => x,
                _ => new ClusterNodeStatusReport(new ServerReport(), report, ClusterNodeStatusReport.ReportStatus.Ok, null, DateTime.UtcNow, null));
            var state = new ClusterObserver.DatabaseObservationState
            {
                RawDatabase = rawRecord,
                ClusterTopology = serverStore.GetClusterTopology(context),
                DatabaseTopology = rawRecord.Topology,
                Current = current,
                Name = database
            };

            cmd = serverStore.Observer.GetCompareExchangeTombstonesToCleanup(database, state: state, context, out var cleanupState);
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
    }
}
