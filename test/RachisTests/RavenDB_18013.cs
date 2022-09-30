using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
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
    private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(40);
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
            Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: Got '{type}' command");

            if (type == nameof(DeleteDatabaseCommand))
            {
                Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: waiting for '{nameof(deleteDatabaseCommandMre)}'");

                deleteDatabaseCommandHasWaiterMre.Set();
                var result = deleteDatabaseCommandMre.WaitOne(_reasonableWaitTime);

                Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: '{nameof(deleteDatabaseCommandMre)}' got signaled. Result: {result}");

            }
            else if (type == nameof(RemoveNodeFromDatabaseCommand))
            {
                try
                {
                    Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: signaling '{nameof(cde)}'");

                    cde.Signal();
                }
                catch (InvalidOperationException)
                {
       
                }
                Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: waiting for '{nameof(removeNodeFromDatabaseCommandMre)}' (1)");

                var result = removeNodeFromDatabaseCommandMre.WaitOne(_reasonableWaitTime);
                removeNodeFromDatabaseCommandMre.Reset();

                Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: result of waiting for '{nameof(removeNodeFromDatabaseCommandMre)}': {result} (1)");

                if (Interlocked.Increment(ref c) == 1)
                {
                    Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: incremented '{nameof(c)}' to 1 (value: {c}), returning");

                    return;
                }
                else
                {
                    Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: incremented '{nameof(c)}', current value: {c}");
                    Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: waiting for '{nameof(removeNodeFromDatabaseCommandMre)}' (2)");

                    result = removeNodeFromDatabaseCommandMre.WaitOne(_reasonableWaitTime);

                    Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: result of waiting for '{nameof(removeNodeFromDatabaseCommandMre)}': {result} (2)");
                }
            }
        };
        var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        var testingStuff = documentDatabase.ForTestingPurposesOnly();

        using (testingStuff.CallDuringDocumentDatabaseInternalDispose(() =>
               {
                   documentDatabaseDisposeHasWaiterMre.Set();
                   Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: waiting for '{nameof(documentDatabaseDisposeMre)}'");

                   var result = documentDatabaseDisposeMre.WaitOne(_reasonableWaitTime);

                   Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: result of waiting for '{nameof(documentDatabaseDisposeMre)}': {result}");
               }))
        {

            var t = store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true));

            // wait for DeleteDatabaseCommand
            Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: waiting for '{nameof(deleteDatabaseCommandHasWaiterMre)}' in test");

            deleteDatabaseCommandHasWaiterMre.WaitOne(_reasonableWaitTime);
            // wait for RemoveNodeFromDatabaseCommands
            Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: waiting for '{nameof(cde)}' in test");

            cde.Wait(_reasonableWaitTime);
            // advance DeleteDatabaseCommand
            Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: waiting for '{nameof(deleteDatabaseCommandMre)}' in test");

            deleteDatabaseCommandMre.Set();
            // wait for the thread to reach dispose of document database
            Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: waiting for '{nameof(documentDatabaseDisposeHasWaiterMre)}' in test");

            documentDatabaseDisposeHasWaiterMre.WaitOne(_reasonableWaitTime);
            // advance one of RemoveNodeFromDatabaseCommands
            removeNodeFromDatabaseCommandMre.Set();
            // we should handle deletion while deletion is in progress
            Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: waiting for 'DeleteDatabaseWhileItBeingDeleted' in test");

            server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().DeleteDatabaseWhileItBeingDeleted.WaitOne(_reasonableWaitTime);

            // advance the document database dispose & RemoveNodeFromDatabaseCommands
            documentDatabaseDisposeMre.Set();
            removeNodeFromDatabaseCommandMre.Set();

            // finish delete
            try
            {
                await t;
            }
            catch (Exception e)
            {
                Output.WriteLine($"{SystemTime.UtcNow} RavenDB-18013: Failed to delete database from test {nameof(ShouldHandleDatabaseDeleteWhileItsBeingDeleted)}:{e}");
                throw;
            }

            // we're done with the disposal - let's signal all MREs to ensure the test will not hang on the disposal

            deleteDatabaseCommandMre.Set();
            deleteDatabaseCommandHasWaiterMre.Set();
            removeNodeFromDatabaseCommandMre.Set();
            documentDatabaseDisposeMre.Set();
            documentDatabaseDisposeHasWaiterMre.Set();
        }
    }
}
