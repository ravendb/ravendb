using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Commands;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests;

public class RavenDB_18013 : ClusterTestBase
{
    public RavenDB_18013(ITestOutputHelper output) : base(output)
    {
    }
    private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);
    [Fact]
    public async Task ShouldHandleDatabaseDeleteWhileItsBeingDeleted()
    {
        var cluster = await CreateRaftCluster(3, shouldRunInMemory: false);

        var deleteDatabaseCommandMre = new ManualResetEvent(false);
        var deleteDatabaseCommandHasWaiterMre = new ManualResetEvent(false);
        var removeNodeFromDatabaseCommandMre = new ManualResetEvent(false);
        var documentDatabaseDisposeMre = new ManualResetEvent(false);
        var documentDatabaseDisposeHasWaiterMre = new ManualResetEvent(false);

        using var store = GetDocumentStore(new Options
        {
            Server = cluster.Leader,
            ReplicationFactor = 3,
            ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin,
            RunInMemory = false,
            DeleteDatabaseOnDispose = false
        });

        CountdownEvent cde = new CountdownEvent(2);
        var c = 0;
        var server = cluster.Nodes.First();
        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().DeleteDatabaseWhileItBeingDeleted = new ManualResetEvent(false);
        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().InsideHandleClusterDatabaseChanged += type =>
        {
            if (type == nameof(DeleteDatabaseCommand))
            {
                deleteDatabaseCommandHasWaiterMre.Set();
                deleteDatabaseCommandMre.WaitOne(_reasonableWaitTime);
            }
            else if (type == nameof(RemoveNodeFromDatabaseCommand))
            {
                try
                {
                    cde.Signal();
                }
                catch (InvalidOperationException)
                {
       
                }
                removeNodeFromDatabaseCommandMre.WaitOne(_reasonableWaitTime);
                removeNodeFromDatabaseCommandMre.Reset();
                if (Interlocked.Increment(ref c) == 1)
                {
                    return;
                }
                else
                {
                    removeNodeFromDatabaseCommandMre.WaitOne(_reasonableWaitTime);
                }
            }
        };
        var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        var testingStuff = documentDatabase.ForTestingPurposesOnly();

        using (testingStuff.CallDuringDocumentDatabaseInternalDispose(() =>
               {
                   documentDatabaseDisposeHasWaiterMre.Set();
                   documentDatabaseDisposeMre.WaitOne(_reasonableWaitTime);
               }))
        {

            var t = store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true));

            // wait for DeleteDatabaseCommand
            deleteDatabaseCommandHasWaiterMre.WaitOne(_reasonableWaitTime);
            // wait for RemoveNodeFromDatabaseCommands
            cde.Wait(_reasonableWaitTime);
            // advance DeleteDatabaseCommand
            deleteDatabaseCommandMre.Set();
            // wait for the thread to reach dispose of document database
            documentDatabaseDisposeHasWaiterMre.WaitOne(_reasonableWaitTime);
            // advance one of RemoveNodeFromDatabaseCommands
            removeNodeFromDatabaseCommandMre.Set();
            // we should handle deletion while deletion is in progress
            server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().DeleteDatabaseWhileItBeingDeleted.WaitOne(_reasonableWaitTime);

            // advance the document database dispose & RemoveNodeFromDatabaseCommands
            documentDatabaseDisposeMre.Set();
            removeNodeFromDatabaseCommandMre.Set();

            // finish delete
            await t;
        }
    }
}
