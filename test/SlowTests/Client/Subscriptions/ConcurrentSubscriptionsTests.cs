using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server;
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

                    var con1Docs = new List<string>();
                    var con2Docs = new List<string>();

                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con1Docs.Add(item.Id);
                        }
                    });

                    var _ = secondSubscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con2Docs.Add(item.Id);
                        }
                    });

                    await AssertWaitForTrueAsync(() => Task.FromResult(con1Docs.Count + con2Docs.Count == 6), 6000);
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

                    var con1Docs = new List<string>();
                    var con2Docs = new List<string>();

                    var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    var mre = new ManualResetEvent(false);

                    var _ = Subscription2.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con2Docs.Add(item.Id);
                        }

                        mre.Set();
                        tcs.Task.Wait();
                    });

                    mre.WaitOne();

                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con1Docs.Add(item.Id);
                        }
                    });

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(con2Docs.Count == 2), true, 6000, 100), $"connection 2 has {con2Docs.Count} docs");
                    Assert.True(await WaitForValueAsync(() => Task.FromResult(con1Docs.Count == 4), true, 6000, 100), $"connection 1 has {con1Docs.Count} docs");

                    WaitForUserToContinueTheTest(store);

                    tcs.SetException(new InvalidOperationException());
                    await Subscription2.DisposeAsync(waitForSubscriptionTask: true);

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(con1Docs.Count == 6), true, 6000, 100), $"connection 1 has {con1Docs.Count} docs");
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
                await using (var subscription2 = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
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

                    var con1Docs = new List<(string id, string name)>();
                    var con2Docs = new List<(string id, string name)>();

                    var delayConn2ack = new AsyncManualResetEvent();
                    var waitUntilConn2GetsUser1 = new AsyncManualResetEvent();
                    var waitBeforeConn1FinishesSecondBatch = new AsyncManualResetEvent();
                    var batchesProcessedByConn1 = 0;

                    var _ = subscription2.Run(async x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con2Docs.Add((item.Id, item.Result.Name));

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
                            con1Docs.Add((item.Id, item.Result.Name));
                        }

                        batchesProcessedByConn1++;
                        if (batchesProcessedByConn1 == 1)
                        {
                            waitUntilConn1FinishesBatch.Set();
                            await waitBeforeConn1FinishesSecondBatch.WaitAsync();
                        }
                    });
                    await waitUntilConn1FinishesBatch.WaitAsync(_reasonableWaitTime);
                    Assert.Contains(("user/2", null), con1Docs);
                    Assert.Contains(("user/3", null), con1Docs);
                    Assert.DoesNotContain(("user/1", "NotChanged"), con1Docs);
                    Assert.DoesNotContain(("user/1", "Changed"), con1Docs);

                    waitBeforeConn1FinishesSecondBatch.Set();
                    delayConn2ack.Set(); // let connection 2 finish with old user/1

                    Assert.Contains(("user/1", "NotChanged"), con2Docs);

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(con1Docs.Contains(("user/1", "Changed")) || con2Docs.Contains(("user/1", "Changed"))), true, 6000, 100), $"connection 1 and 2 are missing new user/1");
                    Assert.False(await WaitForValueAsync(() => Task.FromResult(con1Docs.Contains(("user/1", "Changed")) && con2Docs.Contains(("user/1", "Changed"))), true, 6000, 100), $"connection 1 and 2 both got new user/1");

                    await AssertNoLeftovers(store, id);
                }
            }
        }

        [Fact]
        public async Task RemoveDeletedDocumentFromResend()
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
                await using (var subscription2 = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
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

                    var con1Docs = new List<string>();
                    var con2Docs = new List<string>();

                    var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    var mre = new ManualResetEvent(false);

                    var _ = subscription2.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con2Docs.Add(item.Id);
                        }

                        mre.Set();
                        tcs.Task.Wait();
                    });

                    mre.WaitOne();

                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete("users/1");
                        await session.StoreAsync(new User(), "users/7");
                        await session.SaveChangesAsync();
                    }

                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con1Docs.Add(item.Id);
                        }
                    });

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(con2Docs.Count == 2), true, 6000, 100), $"connection 2 has {con2Docs.Count} docs");
                    Assert.True(await WaitForValueAsync(() => Task.FromResult(con1Docs.Count == 5), true, 6000, 100), $"connection 1 has {con1Docs.Count} docs");

                    Assert.Contains("users/7", con1Docs);
                    tcs.SetException(new InvalidOperationException());

                    await WaitForNoExceptionAsync(() => AssertNoLeftovers(store, id));
                }
            }
        }

        public async Task WaitForNoExceptionAsync(Func<Task> task, TimeSpan? timeToWait = null)
        {
            timeToWait ??= TimeSpan.FromSeconds(15);
            var sp = Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    await task();
                    return;
                }
                catch
                {
                    if (sp.Elapsed > timeToWait)
                        throw;

                    await Task.Delay(25);
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

                    var con1Docs = new List<string>();
                    var con2Docs = new List<string>();

                    var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    var mre = new ManualResetEvent(false);

                    var _ = Subscription2.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con2Docs.Add(item.Id);
                        }

                        mre.Set();
                        tcs.Task.Wait();
                    });

                    mre.WaitOne();

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Changed" }, "users/1");
                        await session.StoreAsync(new User(), "users/7");
                        await session.SaveChangesAsync();
                    }

                    var gotIt = false;
                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con1Docs.Add(item.Id);
                            if (item.Result.Name == "Changed")
                            {
                                gotIt = true;
                            }
                        }
                    });

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(con2Docs.Count == 2), true, 6000, 100), $"connection 2 has {con2Docs.Count} docs");
                    Assert.True(await WaitForValueAsync(() => Task.FromResult(con1Docs.Count == 5), true, 6000, 100), $"connection 1 has {con1Docs.Count} docs");

                    Assert.Contains("users/7", con1Docs);
                    tcs.SetException(new InvalidOperationException());

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(gotIt), true), $"updated document didn't arrived");

                    await AssertNoLeftovers(store, id);
                }
            }
        }

        private async Task AssertNoLeftovers(IDocumentStore store, string id)
        {
            var db = await GetDocumentDatabaseInstanceFor(store);

            await AssertWaitForValueAsync(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    return Task.FromResult(db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, id).GetNumberOfResendDocuments(SubscriptionType.Document));
                }
            }, 0);
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

                    var con1Docs = new List<string>();
                    var con2Docs = new List<string>();

                    var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    var mre = new ManualResetEvent(false);

                    var _ = Subscription2.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con2Docs.Add(item.Id);
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
                            con1Docs.Add(item.Id);
                            if (item.Result.Name == "Changed")
                            {
                                gotIt = true;
                            }
                        }
                    });

                    Assert.True(await WaitForValueAsync(() => Task.FromResult(con2Docs.Count == 2), true, 6000, 100), $"connection 2 has {con2Docs.Count} docs");
                    Assert.True(await WaitForValueAsync(() => Task.FromResult(con1Docs.Count == 5), true, 6000, 100), $"connection 1 has {con1Docs.Count} docs");

                    Assert.Contains("users/7", con1Docs);
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
                ModifyDocumentStore = s => s.Conventions.LoadBalanceBehavior = LoadBalanceBehavior.UseSessionContext
            });

            var database = store.Database;

            var node1 = await cluster.Nodes[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var node2 = await cluster.Nodes[1].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var node3 = await cluster.Nodes[2].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);

            var t1 = await BreakReplication(cluster.Nodes[0].ServerStore, database);
            var t2 = await BreakReplication(cluster.Nodes[1].ServerStore, database);
            var t3 = await BreakReplication(cluster.Nodes[2].ServerStore, database);

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
                await session.SaveChangesAsync();
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
                var con1Docs = new List<string>();
                var con2Docs = new List<string>();
                var mre = new ManualResetEventSlim(false);
                var isDown = false;
                int lockServerDisposal = 0;
                subscription.AfterAcknowledgment += b =>
                {
                    if (b.NumberOfItemsInBatch > 0)
                    {
                        Interlocked.Increment(ref lockServerDisposal);
                        if (lockServerDisposal == 1)
                        {
                            if (isDown == false)
                            {
                                isDown = true;
                                mre.Set();
                            }
                        }
                        Interlocked.Decrement(ref lockServerDisposal);
                    }

                    return Task.CompletedTask;
                };
                subscription2.AfterAcknowledgment += b =>
                {
                    if (b.NumberOfItemsInBatch > 0)
                    {
                        Interlocked.Increment(ref lockServerDisposal);
                        if (lockServerDisposal == 1)
                        {
                            if (isDown == false)
                            {
                                isDown = true;
                                mre.Set();
                            }
                        }
                        Interlocked.Decrement(ref lockServerDisposal);
                    }

                    return Task.CompletedTask;
                };

                var t = subscription.Run(x =>
                {
                    foreach (var item in x.Items)
                    {
                        con1Docs.Add(item.Id);
                    }
                });

                var _ = subscription2.Run(x =>
                {
                    foreach (var item in x.Items)
                    {
                        con2Docs.Add(item.Id);
                    }
                });

                await AssertWaitForTrueAsync(() => Task.FromResult(con1Docs.Count + con2Docs.Count == 6), 6000, 100);
            }
        }

        [Fact]
        public async Task DropSingleConnection()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                using (var worker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    MaxDocsPerBatch = 2
                }))
                using (var worker2 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
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

                    var mre1 = new AsyncManualResetEvent();
                    var mre2 = new AsyncManualResetEvent();

                    worker.OnEstablishedSubscriptionConnection += () =>
                    {
                        mre1.Set();
                    };
                    worker2.OnEstablishedSubscriptionConnection += () =>
                    {
                        mre2.Set();
                    };

                    var con1Docs = new List<string>();
                    var con2Docs = new List<string>();

                    var t = worker.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con1Docs.Add(item.Id);
                        }
                    });

                    var _ = worker2.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con2Docs.Add(item.Id);
                        }
                    });

                    await mre1.WaitAsync();
                    await mre2.WaitAsync();

                    store.Subscriptions.DropSubscriptionWorker(worker2);
                    var db = await GetDocumentDatabaseInstanceFor(store);
                    db.SubscriptionStorage.TryGetRunningSubscriptionConnectionsState(long.Parse(id), out var subscriptionConnectionsState);
                    Assert.Equal(1, subscriptionConnectionsState.GetConnections().Count);
                    await AssertWaitForTrueAsync(() => Task.FromResult(con1Docs.Count + con2Docs.Count == 6 || con1Docs.Count + con2Docs.Count == 8), 6000);
                    await AssertNoLeftovers(store, id);
                }
            }
        }

        [Fact]
        public async Task ThrowOnInvalidLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                Server.ServerStore.LicenseManager.LicenseStatus.Attributes["concurrentSubscriptions"] = false;

                var id = store.Subscriptions.Create<User>();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                }))
                {
                    var t = subscription.Run(x =>
                    {
                       
                    });

                    var ex = await Assert.ThrowsAsync<SubscriptionInvalidStateException>(() => t.WaitAndThrowOnTimeout(TimeSpan.FromSeconds(15)));
                    Assert.Contains("Your current license doesn't include the Concurrent Subscriptions feature", ex.ToString());
                }
            }
        }

        private class User
        {
            public string Name;
        }
    }
}
