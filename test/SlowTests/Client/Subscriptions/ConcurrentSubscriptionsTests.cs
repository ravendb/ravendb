using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class ConcurrentSubscriptionsTests : ReplicationTestBase
    {
        public ConcurrentSubscriptionsTests(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

        //TODO: test to check only concurrent connections can join concurrent subscription
        //TODO: test for connection disconnects
        
        [Fact]
        public async Task ConcurrentSubscriptions()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    MaxDocsPerBatch = 2
                }))
                using (var secondSubscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    MaxDocsPerBatch = 2
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "user/1");
                        session.Store(new User(), "user/2");
                        session.Store(new User(), "user/3");
                        session.Store(new User(), "user/4");
                        session.Store(new User(), "user/5");
                        session.Store(new User(), "user/6");
                        session.SaveChanges();
                    }

                    var Con1Docs = new List<string>();
                    var Con2Docs = new List<string>();
                    
                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Con1Docs.Add(item.Id);
                        }
                    });
                    
                    var _ = secondSubscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Con2Docs.Add(item.Id);
                        }
                    });

                    await AssertWaitForTrueAsync(() => Task.FromResult(Con1Docs.Count + Con2Docs.Count == 6),6000);
                    await AssertNoLeftovers(store, id);
                }
            }
        }

        [Fact]
        public async Task ResendAfterConnectionClosed()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                await using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    MaxDocsPerBatch = 2
                }))
                await using (var Subscription2 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    MaxDocsPerBatch = 2
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "user/1");
                        session.Store(new User(), "user/2");
                        session.Store(new User(), "user/3");
                        session.Store(new User(), "user/4");
                        session.Store(new User(), "user/5");
                        session.Store(new User(), "user/6");
                        session.SaveChanges();
                    }

                    var Con1Docs = new List<string>();
                    var Con2Docs = new List<string>();

                    var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    var mre = new ManualResetEvent(false);

                    var _ = Subscription2.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Con2Docs.Add(item.Id);
                        }

                        mre.Set();
                        tcs.Task.Wait();
                    });

                    mre.WaitOne();

                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Con1Docs.Add(item.Id);
                        }
                    });
                    
                    Assert.True(await WaitForValueAsync(() => Task.FromResult(Con2Docs.Count == 2), true, 6000, 100), $"connection 2 has {Con2Docs.Count} docs");
                    Assert.True(await WaitForValueAsync(() => Task.FromResult(Con1Docs.Count == 4), true, 6000, 100), $"connection 1 has {Con1Docs.Count} docs");
                    tcs.SetException(new InvalidOperationException());
                    await Subscription2.DisposeAsync(waitForSubscriptionTask: true);

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(Con1Docs.Count == 6), true, 6000, 100), $"connection 1 has {Con1Docs.Count} docs");
                    await AssertNoLeftovers(store, id);
                }
            }
        }

        [Fact]
        public async Task ResendWhenDocumentIsProcessedByAnotherConnection()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    MaxDocsPerBatch = 2
                }))
                await using (var Subscription2 = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    MaxDocsPerBatch = 2
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        var user = new User();
                        user.Name = "NotChanged";
                        session.Store(user, "user/1");
                        session.SaveChanges();
                    }

                    var Con1Docs = new List<(string id, string name)>();
                    var Con2Docs = new List<(string id, string name)>();
                    
                    var delayConn2ack = new AsyncManualResetEvent();
                    var waitUntilConn2GetsUser1 = new AsyncManualResetEvent();
                    var waitBeforeConn1FinishesSecondBatch = new AsyncManualResetEvent();
                    var batchesProcessedByConn1 = 0;

                    var _ = Subscription2.Run(async x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Console.WriteLine("connection 2: " + item.Id + " name: " + ((User)item.Result).Name);

                            Con2Docs.Add((item.Id, item.Result.Name));

                            waitUntilConn2GetsUser1.Set();
                            await delayConn2ack.WaitAsync(_reasonableWaitTime);
                        }
                    });

                    await waitUntilConn2GetsUser1.WaitAsync(_reasonableWaitTime);

                    using (var session = store.OpenSession())
                    {
                        var user1 = session.Load<User>("user/1");
                        user1.Name = "Changed";
                        session.Store(new User(), "user/2");
                        session.Store(new User(), "user/3");

                        session.SaveChanges();
                    }

                    var waitUntilConn1FinishesBatch = new AsyncManualResetEvent();
                    var t = subscription.Run(async x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Con1Docs.Add((item.Id, item.Result.Name));

                            Console.WriteLine("connection 1: " + item.Id + " name: " + ((User)item.Result).Name);
                        }

                        batchesProcessedByConn1++;
                        if (batchesProcessedByConn1 == 2)
                        {
                            waitUntilConn1FinishesBatch.Set();
                            await waitBeforeConn1FinishesSecondBatch.WaitAsync();
                        }
                    });
                    await waitUntilConn1FinishesBatch.WaitAsync(_reasonableWaitTime);
                    Assert.Contains(("user/2", null), Con1Docs);
                    Assert.Contains(("user/3", null), Con1Docs);
                    Assert.DoesNotContain(("user/1", "NotChanged"), Con1Docs);
                    Assert.DoesNotContain(("user/1", "Changed"), Con1Docs);

                    waitBeforeConn1FinishesSecondBatch.Set();
                    delayConn2ack.Set(); // let connection 2 finish with old user/1
                    
                    Assert.Contains(("user/1", "NotChanged"), Con2Docs);

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(Con1Docs.Contains(("user/1", "Changed")) || Con2Docs.Contains(("user/1", "Changed"))), true, 6000, 100), $"connection 1 and 2 are missing new user/1");
                    Assert.False(await WaitForValueAsync(() => Task.FromResult(Con1Docs.Contains(("user/1", "Changed")) && Con2Docs.Contains(("user/1", "Changed"))), true, 6000, 100), $"connection 1 and 2 both got new user/1");

                    await AssertNoLeftovers(store, id);
                }
            }
        }

        [Fact]
        public async Task ResendChangedDocument()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    MaxDocsPerBatch = 2
                }))
                await using (var Subscription2 = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    MaxDocsPerBatch = 2
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "users/1");
                        session.Store(new User(), "users/2");
                        session.Store(new User(), "users/3");
                        session.Store(new User(), "users/4");
                        session.Store(new User(), "users/5");
                        session.Store(new User(), "users/6");
                        session.SaveChanges();
                    }

                    var Con1Docs = new List<string>();
                    var Con2Docs = new List<string>();

                    var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    var mre = new ManualResetEvent(false);

                    var _ = Subscription2.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Con2Docs.Add(item.Id);
                        }

                        mre.Set();
                        tcs.Task.Wait();
                    });

                    mre.WaitOne();

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User {Name = "Changed"}, "users/1");
                        await session.StoreAsync(new User (), "users/7");
                        await session.SaveChangesAsync();
                    }

                    var gotIt = false;
                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Con1Docs.Add(item.Id);
                            if (item.Result.Name == "Changed")
                            {
                                gotIt = true;
                            }
                        }
                    });

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(Con2Docs.Count == 2), true, 6000, 100), $"connection 2 has {Con2Docs.Count} docs");
                    Assert.True(await WaitForValueAsync(() => Task.FromResult(Con1Docs.Count == 5), true, 6000, 100), $"connection 1 has {Con1Docs.Count} docs");

                    Assert.Contains("users/7", Con1Docs);
                    tcs.SetException(new InvalidOperationException());

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(gotIt), true), $"updated document didn't arrived");

                    await AssertNoLeftovers(store, id);
                }
            }
        }

        private async Task AssertNoLeftovers(IDocumentStore store, string id)
        {
            var db = await GetDocumentDatabaseInstanceFor(store);

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var leftovers = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, id).GetNumberOfResendDocuments();
                Assert.Equal(0L, leftovers);
            }
        }

        [Fact]
        public async Task ResendChangedDocument2()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    MaxDocsPerBatch = 2
                }))
                await using (var Subscription2 = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    MaxDocsPerBatch = 2
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "users/1");
                        session.Store(new User(), "users/2");
                        session.Store(new User(), "users/3");
                        session.Store(new User(), "users/4");
                        session.Store(new User(), "users/5");
                        session.Store(new User(), "users/6");
                        session.SaveChanges();
                    }

                    var Con1Docs = new List<string>();
                    var Con2Docs = new List<string>();

                    var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    var mre = new ManualResetEvent(false);

                    var _ = Subscription2.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Con2Docs.Add(item.Id);
                        }

                        mre.Set();
                        tcs.Task.Wait();
                    });

                    mre.WaitOne();
                    
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Changed" }, "users/1");
                        await session.StoreAsync(new User { Name = "Changed" }, "users/2");
                        await session.StoreAsync(new User(), "users/7");
                        await session.SaveChangesAsync();
                    }

                    var gotIt = false;
                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Con1Docs.Add(item.Id);
                            if (item.Result.Name == "Changed")
                            {
                                gotIt = true;
                            }
                        }
                    });

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(Con2Docs.Count == 2), true, 6000, 100), $"connection 2 has {Con2Docs.Count} docs");
                    Assert.True(await WaitForValueAsync(() => Task.FromResult(Con1Docs.Count == 5), true, 6000, 100), $"connection 1 has {Con1Docs.Count} docs");

                    Assert.Contains("users/7", Con1Docs);
                    tcs.SetException(new InvalidOperationException());

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(gotIt), true), $"updated document didn't arrived");
                    await AssertNoLeftovers(store, id);
                }
            }
        }

        [Fact]
        public async Task ConcurrentSubscriptionMultipleNodes()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            
            using var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 3,
                Server = cluster.Leader,
                ModifyDocumentStore = s =>s.Conventions.LoadBalanceBehavior = LoadBalanceBehavior.UseSessionContext
            });
            
            var database = store.Database;
            /*using var store2 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[1].WebUrl },
               // Conventions = new DocumentConventions { DisableTopologyUpdates = true }
            }.Initialize();

            using var store3 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[2].WebUrl },
               // Conventions = new DocumentConventions { DisableTopologyUpdates = true }
            }.Initialize();*/

            /*var allStores = new[] { (DocumentStore)store1, (DocumentStore)store2, (DocumentStore)store3 };
            var toDelete = cluster.Nodes.First(n => n != cluster.Leader);
            var toBeDeletedStore = allStores.Single(s => s.Urls[0] == toDelete.WebUrl);
            var nonDeletedStores = allStores.Where(s => s.Urls[0] != toDelete.WebUrl).ToArray();
            var nonDeletedNodes = cluster.Nodes.Where(n => n.ServerStore.NodeTag != toDelete.ServerStore.NodeTag).ToArray();
            var deletedNode = cluster.Nodes.Single(n => n.ServerStore.NodeTag == toDelete.ServerStore.NodeTag);*/

            var node1 = await cluster.Nodes[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var node2 = await cluster.Nodes[1].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var node3 = await cluster.Nodes[2].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);

            var t1 = await BreakReplication(cluster.Nodes[0].ServerStore, database);
            var t2 = await BreakReplication(cluster.Nodes[1].ServerStore, database);
            var t3 = await BreakReplication(cluster.Nodes[2].ServerStore, database);

            /*using (var node1Controller = new ReplicationTestBase.ReplicationController(node1))
            using (var node2Controller = new ReplicationTestBase.ReplicationController(node2))
            using (var node3Controller = new ReplicationTestBase.ReplicationController(node3))*/
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.SessionInfo.SetContext("foo");
                    await session.StoreAsync(new User(), "user/1");
                    await session.StoreAsync(new User(), "user/2");
                    await session.StoreAsync(new User(), "user/3");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.SessionInfo.SetContext("bar");
                    await session.StoreAsync(new User(), "user/4");
                    await session.StoreAsync(new User(), "user/5");
                    await session.StoreAsync(new User(), "user/6");
                    //await session.StoreAsync(new User(), "user/7");
                    //await session.StoreAsync(new User(), "user/8");
                    //await session.StoreAsync(new User(), "user/9");
                    await session.SaveChangesAsync();
                }

                /*node1Controller.ReplicateOnce();
                node2Controller.ReplicateOnce();*/
            }

            t1.Mend();
            t2.Mend();
            t3.Mend();

            await WaitForDocumentInClusterAsync<User>(cluster.Nodes, database, "user/6", predicate: null, TimeSpan.FromSeconds(15));
            await WaitForDocumentInClusterAsync<User>(cluster.Nodes, database, "user/3", predicate: null, TimeSpan.FromSeconds(15));

            var id = store.Subscriptions.Create<User>(options: new SubscriptionCreationOptions
            {
                MentorNode = "A"
            });
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
            {
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                Strategy = SubscriptionOpeningStrategy.Concurrent,
                MaxDocsPerBatch = 2
            }))
            await using (var subscription2 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
            {
                Strategy = SubscriptionOpeningStrategy.Concurrent, 
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5), 
                MaxDocsPerBatch = 2
            }))
            {
                var Con1Docs = new List<string>();
                var Con2Docs = new List<string>();
                var mre = new ManualResetEventSlim(false);
                var first = true;
                var isDown = false;
                var ackCounter = 0;
                int lockServerDisposal = 0;
                subscription.AfterAcknowledgment += async b =>
                {
                    if (b.NumberOfItemsInBatch > 0)
                    {
                        Interlocked.Increment(ref lockServerDisposal);
                        if (lockServerDisposal == 1)
                        {
                            if (isDown == false)
                            {
                                //var nodeA = cluster.Nodes.Single(s => s.ServerStore.NodeTag == "A");
                                //DisposeServerAndWaitForFinishOfDisposal(nodeA.ServerStore.Server);
                                isDown = true;
                                mre.Set();
                            }

                            ackCounter++;
                        }
                        Interlocked.Decrement(ref lockServerDisposal);
                    }
                };
                subscription2.AfterAcknowledgment += async b =>
                {
                    if (b.NumberOfItemsInBatch > 0)
                    {
                        Interlocked.Increment(ref lockServerDisposal);
                        if (lockServerDisposal == 1)
                        {
                            if (isDown == false)
                            {
                                //var nodeA = cluster.Nodes.Single(s => s.ServerStore.NodeTag == "A");
                                //DisposeServerAndWaitForFinishOfDisposal(nodeA.ServerStore.Server);
                                isDown = true;
                                mre.Set();
                            }

                            ackCounter++;
                        }
                        Interlocked.Decrement(ref lockServerDisposal);
                    }
                };

                var t = subscription.Run(x =>
                {
                    //if second or above, then first ack was sent and need to wait
                    //if (first == false)
                    //{
                    //    mre.Wait();
                    //    first = false;
                    //}

                    foreach (var item in x.Items)
                    {
                        //if (first || isDown)
                        {
                            Con1Docs.Add(item.Id);
                        }
                    }
                });

                var _ = subscription2.Run(x =>
                {
                    //if (first == false)
                    //{
                    //    mre.Wait();
                    //    first = false;
                    //}

                    foreach (var item in x.Items)
                    {
                        //if (first || isDown)
                        {
                            Con2Docs.Add(item.Id);
                        }
                    }
                });

                await AssertWaitForTrueAsync(() => Task.FromResult(Con1Docs.Count + Con2Docs.Count == 6), 6000, 100);
                //await AssertWaitForTrueAsync(() => Task.FromResult(ackCounter >= 3), 6000, 100);
            }
        }

        private class User
        {
            public string Name;
        }
    }
}
