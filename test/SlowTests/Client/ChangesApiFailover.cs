using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class ChangesApiFailover : ClusterTestBase
    {
        public ChangesApiFailover(ITestOutputHelper output) : base(output)
        {
        }


        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        public async Task ChangesApiShouldNotFailOverWhenWaitingForCompletionOfOperation()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
            {
                var (clusterNodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = 3,
                    ModifyDocumentStore = (documentStore => documentStore.Conventions.DisableTopologyUpdates = true) // so request executor stays on the same node
                }))
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    var patch = new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query =
                                $@"
from Users as doc
update {{
    var copy = {{}};
}}
"
                        },
                        new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.FromSeconds(30) }
                    );

                    var expectedNode = (await store.GetRequestExecutor().GetPreferredNode()).Node.ClusterTag;

                    //set up waiting for changes api in WaitForCompletionAsync to set up web socket
                    var database = await Databases.GetDocumentDatabaseInstanceFor(expectedNode, store.Database);
                    var delayQueryByPatch = new AsyncManualResetEvent();
                    database.ForTestingPurposesOnly().DelayQueryByPatch = delayQueryByPatch;

                    var op = await store.Operations.SendAsync(patch, token: cts.Token);
                    Assert.Equal(expectedNode, op.NodeTag);

                    var t = op.WaitForCompletionAsync(cts.Token);

                    var waitWebSocketError = new AsyncManualResetEvent(cts.Token);
                    var changes = store.Changes(store.Database, op.NodeTag);
                    changes.ConnectionStatusChanged += (sender, args) => { waitWebSocketError.Set(); };

                    //wait for websocket to connect
                    await AssertWaitForTrueAsync(() => Task.FromResult(changes.Connected));

                    //bring down server
                    var serverWithPatchOperation = clusterNodes.Single(x => x.ServerStore.NodeTag == expectedNode);
                    var result = await DisposeServerAndWaitForFinishOfDisposalAsync(serverWithPatchOperation);

                    //wait for websocket to throw and retry
                    await waitWebSocketError.WaitAsync(cts.Token);

                    //bring server back up
                    var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                    var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings, NodeTag = result.NodeTag });
                    Servers.Add(server);

                    //run the operation again - should work
                    op = await store.Operations.SendAsync(patch, token: cts.Token);

                    // we reconnect to the same node
                    Assert.Equal(expectedNode, op.NodeTag);
                    await op.WaitForCompletionAsync(cts.Token);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        public async Task ChangesApiForOperationShouldCleanUpFaultyConnection_AfterUnrecoverableError()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
            {
                var (clusterNodes, leader) = await CreateRaftCluster(1, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = 1
                }))
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    var patch = new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query =
                                $@"
from Users as doc
update {{
    var copy = {{}};
}}
"
                        },
                        new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.FromSeconds(30) }
                    );


                    //set up waiting for changes api in WaitForCompletionAsync to set up web socket
                    var database = await Databases.GetDocumentDatabaseInstanceFor(leader.ServerStore.NodeTag, store.Database);
                    var delayQueryByPatch = new AsyncManualResetEvent();
                    database.ForTestingPurposesOnly().DelayQueryByPatch = delayQueryByPatch;

                    var op = await store.Operations.SendAsync(patch, token: cts.Token);

                    var waitWebSocketError = new AsyncManualResetEvent(cts.Token);
                    var changes = store.Changes(store.Database, op.NodeTag);

                    var t = op.WaitForCompletionAsync(cts.Token);

                    //wait for websocket to connect
                    await AssertWaitForTrueAsync(() => Task.FromResult(changes.Connected));

                    //error in changes api will release the mre
                    changes.ConnectionStatusChanged += (sender, args) => { waitWebSocketError.Set(); };

                    //the error will bubble up to user - later await it
                    var waitForCompletionErrorTask = Assert.ThrowsAsync<DatabaseDoesNotExistException>(async () => await t);

                    //delete the database, so when changes api tries reconnecting it will throw db does not exist exception
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: false));

                    // bring server down - so when it reconnects, api will realize db doesn't exist anymore
                    var result = await DisposeServerAndWaitForFinishOfDisposalAsync(leader);

                    //wait for the websocket failure
                    await waitWebSocketError.WaitAsync(cts.Token);

                    //bring server back up
                    var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                    var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings, NodeTag = result.NodeTag });
                    Servers.Add(server);

                    //wait for websocket to throw upon retry connecting - this should result in DatabaseChanges.Dispose - remove connection from the dict
                    await waitForCompletionErrorTask;

                    //create the db again
                    await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database)));

                    //run the operation again - the connection should have been removed from the _databaseChanges dictionary and a new one created
                    op = await store.Operations.SendAsync(patch, token: cts.Token);

                    // we reconnect
                    await op.WaitForCompletionAsync(cts.Token);
                }
            }
        }
        
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        public async Task ChangesApiShouldNotThrowOnConnectionError()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
            {
                var (clusterNodes, leader) = await CreateRaftCluster(1, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = 1
                }))
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    var patch = new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query =
                                $@"
from Users as doc
update {{
    var copy = {{}};
}}
"
                        },
                        new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.FromSeconds(30) }
                    );


                    //set up waiting for changes api in WaitForCompletionAsync to set up web socket
                    var database = await Databases.GetDocumentDatabaseInstanceFor(leader.ServerStore.NodeTag, store.Database);
                    var delayQueryByPatch = new AsyncManualResetEvent();
                    database.ForTestingPurposesOnly().DelayQueryByPatch = delayQueryByPatch;

                    var op = await store.Operations.SendAsync(patch, token: cts.Token);

                    var changes = store.Changes(store.Database, op.NodeTag);

                    //this error should not bubble up to user
                    var waitForCompletionErrorTask = Assert.ThrowsAsync<WebSocketException>(async () =>
                    {
                        await op.WaitForCompletionAsync(cts.Token);
                    });

                    //wait for websocket to connect
                    await AssertWaitForTrueAsync(() => Task.FromResult(changes.Connected));

                    //give WaitForCompletion some time to send the request for the operation in Process before the server goes down
                    await Task.Delay(500);

                    // bring server down
                    var result = await DisposeServerAndWaitForFinishOfDisposalAsync(leader);

                    //bring server back up
                    var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                    var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings, NodeTag = result.NodeTag });
                    Servers.Add(server);

                    //run the operation again
                    op = await store.Operations.SendAsync(patch, token: cts.Token);
                    var resTask = op.WaitForCompletionAsync(cts.Token);

                    var task = await Task.WhenAny(waitForCompletionErrorTask, resTask);

                    Assert.True(task.IsCompletedSuccessfully, $"task {task} hasn't completed successfully. ex: {task.Exception}");
                    Assert.Equal(resTask, task);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        public async Task ChangesApiMonitoringMultipleShouldNotFailAllWhenOneTimesOut()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
            {
                var (clusterNodes, leader) = await CreateRaftCluster(1, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = 1
                }))
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    var patch = new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query =
                                $@"
from Users as doc
update {{
    var copy = {{}};
}}
"
                        },
                        new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.FromSeconds(30) }
                    );

                    //set up waiting for changes api in WaitForCompletionAsync to set up web socket
                    var database = await Databases.GetDocumentDatabaseInstanceFor(leader.ServerStore.NodeTag, store.Database);
                    var delayQueryByPatch = new AsyncManualResetEvent();
                    database.ForTestingPurposesOnly().DelayQueryByPatch = delayQueryByPatch;

                    var op = await store.Operations.SendAsync(patch, token: cts.Token);

                    var list = new BlockingCollection<DocumentChange>();

                    var changes = store.Changes(store.Database, op.NodeTag);
                    await changes.EnsureConnectedNow();
                    var documentsObservable = changes.ForAllDocuments();
                    documentsObservable.Subscribe(list.Add);
                    await documentsObservable.EnsureSubscribedNow();

                    //wait for websocket to connect
                    await AssertWaitForTrueAsync(() => Task.FromResult(changes.Connected));

                    using var ctsToFail = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                    //this will throw on timeout
                    var waitForCompletionErrorTask = Assert.ThrowsAsync<TimeoutException>(async () =>
                    {
                        await op.WaitForCompletionAsync(ctsToFail.Token);
                    });

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1", cts.Token);
                        await session.SaveChangesAsync(cts.Token);
                    }

                    await AssertWaitForTrueAsync(() => Task.FromResult(list.Count == 1));

                    //make sure operation throws an error
                    ctsToFail.Cancel();
                    await waitForCompletionErrorTask;

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/2", cts.Token);
                        await session.SaveChangesAsync(cts.Token);
                    }

                    // make sure document monitoring is still working
                    await AssertWaitForTrueAsync(() => Task.FromResult(list.Count == 2));
                }
            }
        }
        
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        public async Task ChangesApiMonitoringMultipleShouldNotFailAllWhenOneFailsOnFetch()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
            {
                var (clusterNodes, leader) = await CreateRaftCluster(1, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = 1
                }))
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    var patch = new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query =
                                $@"
from Users as doc
update {{
    var copy = {{}};
}}
"
                        },
                        new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.FromSeconds(30) }
                    );

                    //set up waiting for changes api in WaitForCompletionAsync to set up web socket
                    var database = await Databases.GetDocumentDatabaseInstanceFor(leader.ServerStore.NodeTag, store.Database);
                    var delayQueryByPatch = new AsyncManualResetEvent();
                    database.ForTestingPurposesOnly().DelayQueryByPatch = delayQueryByPatch;

                    var re = store.GetRequestExecutor();
                    var delayFetchOperationStatus = new Nito.AsyncEx.AsyncManualResetEvent();
                    re.ForTestingPurposesOnly().WaitBeforeFetchOperationStatus = delayFetchOperationStatus;

                    var op = await store.Operations.SendAsync(patch, token: cts.Token);

                    var list = new BlockingCollection<DocumentChange>();

                    var changes = store.Changes(store.Database, op.NodeTag);
                    await changes.EnsureConnectedNow();
                    var documentsObservable = changes.ForAllDocuments();
                    documentsObservable.Subscribe(list.Add);
                    await documentsObservable.EnsureSubscribedNow();

                    //wait for websocket to connect
                    await AssertWaitForTrueAsync(() => Task.FromResult(changes.Connected));

                    //this will throw on timeout
                    var waitForCompletionErrorTask = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await op.WaitForCompletionAsync();
                    });

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1", cts.Token);
                        await session.SaveChangesAsync(cts.Token);
                    }

                    await AssertWaitForTrueAsync(() => Task.FromResult(list.Count == 1));

                    // bring server down
                    var result = await DisposeServerAndWaitForFinishOfDisposalAsync(leader);

                    //bring server back up
                    var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                    var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings, NodeTag = result.NodeTag });
                    Servers.Add(server);

                    //should fail on fetch operation status in Process because after server restart the request to watch the operation id is no longer in memory
                    delayFetchOperationStatus.Set();
                    await waitForCompletionErrorTask;

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/2", cts.Token);
                        await session.SaveChangesAsync(cts.Token);
                    }

                    // make sure document monitoring is still working
                    await AssertWaitForTrueAsync(() => Task.FromResult(list.Count == 2));
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        public async Task ChangesApiShouldNotIndependentlyFailOverWhenNodeTagSpecified_OnlyRequestExecutorWillFailOver()
        {
            // request executor will failover after server is down and changes api will not attempt to failover the same connection, but instead will open a new connection for the new specific node
            // that is in order to avoid having a connection entry in _databaseChanges that contains a certain node as key but the connection inside has failed over to another node
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            {
                var (clusterNodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = 3
                }))
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    var patch = new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query =
                                $@"
from Users as doc
update {{
    var copy = {{}};
}}
"
                        },
                        new QueryOperationOptions { AllowStale = false, StaleTimeout = TimeSpan.FromSeconds(30) }
                    );

                    var expectedNode = (await store.GetRequestExecutor().GetPreferredNode()).Node.ClusterTag;

                    //set up waiting for changes api in WaitForCompletionAsync to set up web socket
                    var database = await Databases.GetDocumentDatabaseInstanceFor(expectedNode, store.Database);
                    var delayQueryByPatch = new AsyncManualResetEvent();
                    database.ForTestingPurposesOnly().DelayQueryByPatch = delayQueryByPatch;

                    var op = await store.Operations.SendAsync(patch, token: cts.Token);
                    Assert.Equal(expectedNode, op.NodeTag);

                    // set up mre for detecting websocket error
                    var waitWebSocketError = new AsyncManualResetEvent(cts.Token);
                    var changes = store.Changes(store.Database, op.NodeTag);
                    await changes.EnsureConnectedNow();
                    changes.ConnectionStatusChanged += (sender, args) => { waitWebSocketError.Set(); };

                    //wait for websocket to connect
                    await AssertWaitForTrueAsync(() => Task.FromResult(changes.Connected));

                    //bring down server
                    var serverWithPatchOperation = clusterNodes.Single(x => x.ServerStore.NodeTag == expectedNode);
                    var result = await DisposeServerAndWaitForFinishOfDisposalAsync(serverWithPatchOperation);

                    await waitWebSocketError.WaitAsync(cts.Token);

                    // Run a request to make request executor fail over to a different node
                    await store.Maintenance.SendAsync(new GetStatisticsOperation(), token: cts.Token);

                    // Bring server back up
                    var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                    var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings, NodeTag = result.NodeTag });
                    Servers.Add(server);

                    var re = store.GetRequestExecutor(store.Database);
                    //wait for executor to have a different preferred node
                    await AssertWaitForTrueAsync(async () => (await re.GetPreferredNode()).Node.ClusterTag != expectedNode);

                    //run the operation again - should work
                    op = await store.Operations.SendAsync(patch, token: cts.Token);

                    // Will attempt the operation again on a different node this time
                    Assert.NotEqual(expectedNode, op.NodeTag);

                    await op.WaitForCompletionAsync(cts.Token);
                }
            }
        }
        
        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task ChangesApiShouldCleanupFaultyConnectionByItself_TrackingSpecificNode()
        {
            // Notice the only possible faulty connection while writing this test is a DatabaseDoesNotExistException in DoWork

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            {
                var (_, leader) = await CreateRaftCluster(3, leaderIndex: 0, shouldRunInMemory: false);

                using (var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = 3
                }))
                {
                    for (int i = 0; i < 2; i++)
                    {
                        (string DataDirectory, string Url, string NodeTag) result = default;

                        //set up node-specific tracking
                        IDatabaseChanges changes = store.Changes(store.Database, leader.ServerStore.NodeTag);

                        var list = new BlockingCollection<DocumentChange>();

                        await changes.EnsureConnectedNow().WithCancellation(cts.Token);
                        var observableWithTask = changes.ForDocument("users/1");
                        observableWithTask.Subscribe(list.Add);
                        await observableWithTask.EnsureSubscribedNow().WithCancellation(cts.Token);

                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(new User(), "users/1");
                            await session.SaveChangesAsync();
                        }

                        // check socket is connected
                        await AssertWaitForTrueAsync(() => Task.FromResult(list.Count == 1));

                        var waitWebSocketError = new AsyncManualResetEvent(cts.Token);
                        changes.ConnectionStatusChanged += (sender, args) =>
                        {
                            waitWebSocketError.Set();
                            throw new Exception("Test exception");
                        };

                        // first run, fail the websocket entirely
                        if (i == 0)
                        {
                            //bring down server
                            result = await DisposeServerAndWaitForFinishOfDisposalAsync(leader);

                            //wait for the exception following socket being closed
                            await waitWebSocketError.WaitAsync(cts.Token);

                            var ex = await Assert.ThrowsAsync<WebSocketException>(async () =>
                                await changes.EnsureConnectedNow().WithCancellation(cts.Token));

                            // Bring server back up
                            var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } };
                            var server = GetNewServer(new ServerCreationOptions
                            {
                                RunInMemory = false,
                                DeletePrevious = false,
                                DataDirectory = result.DataDirectory,
                                CustomSettings = settings,
                                NodeTag = result.NodeTag
                            });
                            Servers.Add(server);

                            // connection should have been disposed and removed from the dictionary
                            // let's access the same entry in the dictionary to make sure the old one got removed and that we aren't using a faulted connection
                            // In the next loop ->
                        }
                    }
                }
            }
        }
    }
}
