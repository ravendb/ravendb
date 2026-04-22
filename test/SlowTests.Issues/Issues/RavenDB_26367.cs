using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26367 : ClusterTestBase
{
    public RavenDB_26367(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.CompareExchange | RavenTestCategory.Cluster)]
    public async Task ObserverShouldRespectTombstoneCleanupIntervalAfterMultiBatchCleanup()
    {
        // Background: the observer cleans compare-exchange tombstones in batches of 8192 per
        // command. A gate (`now - _lastTombstonesCleanupTimeInTicks >= CompareExchangeTombstonesCleanupInterval`)
        // throttles how often the observer looks for work.
        //
        // Before the fix, the internal `_hasMoreTombstones` flag was never reset between
        // observer iterations. Once a single iteration returned hasMore=true (which happens
        // whenever there are more tombstones than fit in a single batch), the flag remained
        // true forever. Because the tick is only updated when `_hasMoreTombstones == false`,
        // `_lastTombstonesCleanupTimeInTicks` would never advance again and the gate would
        // evaluate to true on every observer iteration — effectively running the tombstone
        // cleanup check every cycle instead of waiting for the configured interval.
        //
        // With the fix, the flag is reset at the top of each cleanup loop, so once the queue
        // drains the tick is updated and the gate kicks back in.

        const int tombstoneCount = 9000;          // > 8192 (one batch) => the multi-batch iteration returns hasMore=true.
        const int cleanupIntervalInMin = 10;      // default; long enough that the gate should block all cleanup attempts during our observation window.
        const int observationWindowMs = 5_000;    // Window during which we count observer cleanup attempts.

        var settings = new Dictionary<string, string>
        {
            { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), cleanupIntervalInMin.ToString() }
        };

        var (_, leader) = await CreateRaftCluster(1, customSettings: settings);
        using var store = GetDocumentStore(new Options
        {
            Server = leader
        });

        // Create `tombstoneCount` compare-exchange values, then delete them all. We leave the
        // observer running so that the moment the cluster-transaction gate opens it will see
        // thousands of tombstones to clean — guaranteeing the first cleanup command returns
        // hasMore=true, which is what triggers the bug.
        var indexes = new ConcurrentDictionary<int, long>();
        await Parallel.ForEachAsync(
            Enumerable.Range(0, tombstoneCount),
            new ParallelOptions { MaxDegreeOfParallelism = 32 },
            async (i, _) =>
            {
                var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>($"cx/{i}", i, index: 0));
                Assert.True(res.Successful, $"Failed to put compare-exchange value cx/{i}");
                indexes[i] = res.Index;
            });

        await Parallel.ForEachAsync(
            Enumerable.Range(0, tombstoneCount),
            new ParallelOptions { MaxDegreeOfParallelism = 32 },
            async (i, _) =>
            {
                var res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<int>($"cx/{i}", indexes[i]));
                Assert.True(res.Successful, $"Failed to delete compare-exchange value cx/{i}");
            });

        // Advance the cluster-wide transaction raft index past the tombstone etags so the
        // observer is allowed to clean them.
        using (var session = store.OpenAsyncSession(new SessionOptions
        {
            TransactionMode = TransactionMode.ClusterWide,
        }))
        {
            await session.StoreAsync(new TombstoneBumpMarker());
            await session.SaveChangesAsync();
        }

        long tombstonesBefore;
        using (leader.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            tombstonesBefore = leader.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
        }
        // Some tombstones may already have been cleaned by the observer while we were issuing
        // deletes; all we need is that there are enough left to force at least two cleanup
        // iterations (i.e., at least one multi-batch result).
        Assert.True(tombstonesBefore > 8192, $"Expected more than 8192 tombstones in place before cleanup, got {tombstonesBefore}.");

        // Reset the tick on every poll: the cluster-wide transaction bump propagates to the
        // observer via heartbeat, so a single reset races that heartbeat and can close the
        // gate before any cleanup runs.
        var allDrained = await WaitForValueAsync(() =>
        {
            leader.ServerStore.Observer._lastTombstonesCleanupTimeInTicks = 0;
            using (leader.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                return leader.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database) == 0;
            }
        }, expectedVal: true, timeout: 60_000, interval: 250);

        Assert.True(allDrained, "Expected all compare-exchange tombstones to be drained by the observer.");

        // Give the observer one cycle to advance the tick naturally now that the queue is empty,
        // before we start counting attempts below.
        await Task.Delay(2_000);

        // Now count how many times the observer attempts tombstone cleanup during a short
        // observation window. With the configured interval of `cleanupIntervalInMin` minutes,
        // the gate should block all attempts after the drain — provided the tick was properly
        // updated.
        var cleanupAttempts = 0;
        leader.ServerStore.Observer.ForTestingPurposesOnly().OnDiagnosticLog = message =>
        {
            // This specific diagnostic line is emitted at the top of GetCleanCompareExchangeTombstonesCommand,
            // i.e., once per observer iteration where the cleanup gate was open.
            if (message != null && message.StartsWith("Starting GetCleanCompareExchangeTombstonesCommand"))
                Interlocked.Increment(ref cleanupAttempts);
        };

        try
        {
            await Task.Delay(observationWindowMs);
        }
        finally
        {
            leader.ServerStore.Observer.ForTestingPurposesOnly().OnDiagnosticLog = null;
        }

        // With the fix: the tick was updated after the drain, so the interval gate blocks all
        // cleanup attempts during the observation window => we expect 0 attempts.
        //
        // Without the fix: the stale `_hasMoreTombstones=true` from the multi-batch iteration
        // is never reset, the tick is never updated, and the gate is permanently open. With
        // the 1-second observer sample period we see roughly `observationWindowMs / 1000` attempts.
        Assert.Equal(0, cleanupAttempts);
    }

    private sealed class TombstoneBumpMarker
    {
        public string Id { get; set; }
    }
}
