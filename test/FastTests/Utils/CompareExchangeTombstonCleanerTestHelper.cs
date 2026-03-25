using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Xunit;

namespace FastTests.Utils;

internal abstract class CompareExchangeTombstoneCleanerTestHelper : RavenTestBase
{
    protected CompareExchangeTombstoneCleanerTestHelper(ITestOutputHelper output) : base(output)
    {
    }

    public static async Task<ClusterObserver.CompareExchangeTombstonesCleanupState> Clean(List<RavenServer> nodes, string databaseName, bool ignoreClustrTrx, StringBuilder sb = null)
    {
        var server = await WaitForNotNullAsync(() => Task.FromResult(nodes.SingleOrDefault(x => x.ServerStore.CurrentRachisState is RachisState.Leader)),
            timeout: (int) TimeSpan.FromSeconds(10).TotalMilliseconds,
            interval: (int) TimeSpan.FromMilliseconds(500).TotalMilliseconds);

        sb ??= new StringBuilder();
        var cleanupState = ClusterObserver.CompareExchangeTombstonesCleanupState.InvalidDatabaseObservationState;
        CleanCompareExchangeTombstonesCommand cmd = null;
        Action<string> onDiagnosticLog = message => sb.AppendLine(message);

        var serverStore = server.ServerStore;

        using (serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        {
            await WaitAndAssertForValueAsync(() =>
            {
                using (context.OpenReadTransaction())
                {
                    using (var rawRecord = serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
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
                            AddState(databaseName, rawRecord.Topology);
                        }

                        if (mergedState.IsValid(onDiagnosticLog) == false)
                            return Task.FromResult(false);

                        var beforeCleanupCompareExchangeTombstonesNumber = serverStore.Cluster.GetNumberOfCompareExchangeTombstones(context, databaseName);
                        sb.AppendLine($"Before cleanup: {beforeCleanupCompareExchangeTombstonesNumber} tombstones.");

                        cmd = serverStore.Observer.GetCleanCompareExchangeTombstonesCommand(databaseName, mergedState, context, out cleanupState);

                        return Task.FromResult(true);

                        void AddState(string name, DatabaseTopology topology)
                        {
                            var state = new ClusterObserver.DatabaseObservationState(name, rawRecord, topology, server.ServerStore.GetClusterTopology(context), current,
                                previous,
                                lastIndexModification: 0, observerIteration: 0);
                            if (ignoreClustrTrx)
                            {
                                foreach ((string _, var clusterNodeStatusReport) in state.Current)
                                foreach ((string _, var databaseStatusReport) in clusterNodeStatusReport.Report)
                                    databaseStatusReport.LastClusterWideTransactionRaftIndex = long.MaxValue;
                            }

                            mergedState.AddState(state);
                        }
                    }
                }
            },
                expectedVal: true,
                timeout: (int)TimeSpan.FromSeconds(10).TotalMilliseconds,
                interval: (int)TimeSpan.FromMilliseconds(500).TotalMilliseconds);
        }

        if (cleanupState != ClusterObserver.CompareExchangeTombstonesCleanupState.HasMoreTombstones)
        {
            sb.AppendLine($"Exiting early, cleanupState: {cleanupState}");
            return cleanupState;
        }

        Assert.NotNull(cmd);

        sb.AppendLine($"Sending {nameof(CleanCompareExchangeTombstonesCommand)} with values: Database `{cmd.DatabaseName}`, MaxRaftIndex `{cmd.MaxRaftIndex}, Take `{cmd.Take}`");
        var result = await serverStore.SendToLeaderAsync(cmd);
        await serverStore.Cluster.WaitForIndexNotification(result.Index);

        using (serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            long afterCleanupCompareExchangeTombstonesNumber = serverStore.Cluster.GetNumberOfCompareExchangeTombstones(context, databaseName);
            sb.AppendLine($"After cleanup: {afterCleanupCompareExchangeTombstonesNumber} tombstones.");
        }

        var hasMore = (bool)result.Result;
        return hasMore
            ? ClusterObserver.CompareExchangeTombstonesCleanupState.HasMoreTombstones
            : ClusterObserver.CompareExchangeTombstonesCleanupState.NoMoreTombstones;
    }
}
