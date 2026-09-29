using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Extensions;
using Raven.Server.Utils;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Server;

// A dispose that hangs has to say where it is stuck and what it waits for, in production and in the failure of a test
public class DisposeAuditTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private static readonly TimeSpan WaitTime = TimeSpan.FromSeconds(30);

    [RavenFact(RavenTestCategory.Core)]
    public void SnapshotShowsTheActiveChainsWithTheirDetailAndProgress()
    {
        using var entered = new CountdownEvent(2);
        using var release = new ManualResetEventSlim();

        using var root = DisposeAudit.Begin("server");

        var branches = Enumerable.Range(1, 2).Select(i => Task.Run(() =>
        {
            using (DisposeAudit.Step($"branch {i}"))
            using (DisposeAudit.Step("inner"))
            {
                DisposeAudit.Progress($"stage of {i}");
                entered.Signal();
                release.Wait();
            }
        })).ToArray();

        Assert.True(entered.Wait(WaitTime));

        var snapshot = root.Snapshot();
        Assert.Contains("Dispose of 'server' running for", snapshot);
        foreach (var i in new[] { 1, 2 })
        {
            // each parallel branch is its own chain, from the root down to what it waits for
            Assert.Contains($"server (", snapshot);
            Assert.Contains($"> branch {i} (", snapshot);
            Assert.Contains($"[at 'stage of {i}' since", snapshot);
        }

        release.Set();
        Assert.True(Task.WaitAll(branches, WaitTime));

        snapshot = root.Snapshot();
        Assert.DoesNotContain("branch", snapshot);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void StepsOutsideOfAnAuditDoNothing()
    {
        using (var step = DisposeAudit.Step("orphan"))
            Assert.Null(step);

        DisposeAudit.Progress("nowhere"); // no audit to report to

        var aggregator = new ExceptionAggregator("test");
        var ran = false;
        aggregator.Execute("named step", () => ran = true);
        aggregator.ThrowIfNeeded();
        Assert.True(ran);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void DisposeOfAStorageEnvironmentOnItsOwnIsAuditedToo()
    {
        var path = NewDataPath();
        using var options = StorageEnvironmentOptions.ForPathForTests(path);
        options.DisposeWaitTime = WaitTime;

        var env = new StorageEnvironment(options);
        var tx = env.ReadTransaction();

        // no server dispose around it, as when a database or an index is deleted
        var dispose = Task.Run(env.Dispose);

        Assert.True(SpinWait.SpinUntil(() => DisposeAudit.SnapshotAll()?.Contains("wait for the active transactions") == true, WaitTime), DisposeAudit.SnapshotAll());
        Assert.Contains($"Dispose of 'StorageEnvironment '", DisposeAudit.SnapshotAll());

        tx.Dispose();
        Assert.True(dispose.Wait(WaitTime));
    }

    [RavenFact(RavenTestCategory.Core)]
    public async Task ServerDisposeSaysItWaitsToCloseTheLandlordGuard()
    {
        var server = GetNewServer();
        using var store = GetDocumentStore(new Options { Server = server, ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true });

        using var handlerEntered = new ManualResetEventSlim();
        using var releaseHandler = new ManualResetEventSlim();
        var block = 0;
        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().InsideHandleClusterDatabaseChanged += _ =>
        {
            if (Interlocked.Exchange(ref block, 0) == 0)
                return;

            handlerEntered.Set();
            releaseHandler.Wait(WaitTime);
        };

        // a database record change is handled inside the landlord guard, which the handler now keeps
        Interlocked.Exchange(ref block, 1);
        var change = store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration()));
        Assert.True(handlerEntered.Wait(WaitTime));

        var dispose = Task.Run(server.Dispose);
        try
        {
            Assert.True(SpinWait.SpinUntil(() => server.DisposeAudit?.Snapshot().Contains("close the `_disposing` guard") == true, WaitTime), server.DisposeAudit?.Snapshot());

            var snapshot = server.DisposeAudit.Snapshot();
            Output.WriteLine(snapshot);
            Assert.Contains("> ServerStore (", snapshot);
            Assert.Contains("> DatabasesLandlord (", snapshot);
        }
        finally
        {
            releaseHandler.Set();
        }

        Assert.True(await dispose.WaitWithTimeout(WaitTime));

        try
        {
            await change;
        }
        catch
        {
            // the server went away under the request, which may or may not have been answered by then
        }
    }
}
