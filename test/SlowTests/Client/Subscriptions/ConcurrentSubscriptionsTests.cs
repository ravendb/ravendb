using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
// ReSharper disable ClassNeverInstantiated.Local
// ReSharper disable CollectionNeverUpdated.Local
#pragma warning disable CS0649
#pragma warning disable CS0169

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
        public async Task ConcurrentSubscriptionsManyWorkers()
        {
            var workersAmount = 10;
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();

                var workerToDocsAmount = new Dictionary<SubscriptionWorker<User>, HashSet<string>>();

                for (int i = 0; i < workersAmount; i++)
                {
                    workerToDocsAmount.Add(store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                        Strategy = SubscriptionOpeningStrategy.Concurrent,
                        MaxDocsPerBatch = 1
                    }), new HashSet<string>());
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 100; i++)
                    {
                        session.Store(new User(), $"user/{i}");
                    }
                    session.SaveChanges();
                }

                foreach (var (worker, docs) in workerToDocsAmount)
                {
                    var _ = worker.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            workerToDocsAmount[worker].Add(item.Id);
                        }
                    });
                }

                await AssertWaitForTrueAsync(() => Task.FromResult(workerToDocsAmount.Sum(x => x.Value.Count) == 100), 6000);
                await AssertNoLeftovers(store, id);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ResendAfterConnectionClosed(bool filter)
        {
            using (var store = GetDocumentStore())
            {
                var id = await (filter ? store.Subscriptions.CreateAsync<User>(user => user.Name != "John") : store.Subscriptions.CreateAsync<User>());
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

                    var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                    await Backup.HoldBackupExecutionIfNeededAndInvoke(ts: null, async () =>
                    {
                        var mre = new AsyncManualResetEvent();

                        var _ = Subscription2.Run(async x =>
                        {
                            foreach (var item in x.Items)
                            {
                                con2Docs.Add(item.Id);
                            }

                            mre.Set();
                            await tcs.Task;
                        });

                        await mre.WaitAsync();

                        var exception = string.Empty;
                        var t = subscription.Run(x =>
                        {
                            foreach (var item in x.Items)
                            {
                                if (string.IsNullOrEmpty(exception) && string.IsNullOrEmpty(item.ExceptionMessage) == false)
                                {
                                    exception = item.ExceptionMessage;
                                }

                                con1Docs.Add(item.Id);
                            }
                        });

                        Assert.True(await WaitForValueAsync(() => Task.FromResult(con2Docs.Count == 2), true, 6000, 100), $"connection 2 has {con2Docs.Count} docs");
                        Assert.True(await WaitForValueAsync(() => Task.FromResult(con1Docs.Count == 4), true, 6000, 100), $"connection 1 has {con1Docs.Count} docs");

                        tcs.SetException(new InvalidOperationException());
                        await Subscription2.DisposeAsync(waitForSubscriptionTask: true);

                        Assert.True(await WaitForValueAsync(() => Task.FromResult(con1Docs.Count == 6), true, 6000, 100), $"connection 1 has {con1Docs.Count} docs");
                        Assert.True(string.IsNullOrEmpty(exception), $"string.IsNullOrEmpty(exception): " + exception);

                        await AssertNoLeftovers(store, id);
                    }, tcs);
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

                    var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                    await Backup.HoldBackupExecutionIfNeededAndInvoke(ts: null, async () =>
                    {
                        var mre = new AsyncManualResetEvent();

                        var _ = subscription2.Run(async x =>
                        {
                            foreach (var item in x.Items)
                            {
                                con2Docs.Add(item.Id);
                            }

                            mre.Set();
                            await tcs.Task;
                        });

                        await mre.WaitAsync();

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
                    }, tcs);
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

                    var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                    await Backup.HoldBackupExecutionIfNeededAndInvoke(ts: null, async () =>
                    {
                        var mre = new AsyncManualResetEvent();

                        var _ = Subscription2.Run(async x =>
                        {
                            foreach (var item in x.Items)
                            {
                                con2Docs.Add(item.Id);
                            }

                            mre.Set();
                            await tcs.Task;
                        });

                        await mre.WaitAsync();

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
                    }, tcs);
                }
            }
        }

        private async Task AssertNoLeftovers(IDocumentStore store, string id)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

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

                    var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                    await Backup.HoldBackupExecutionIfNeededAndInvoke(ts: null, async () =>
                    {
                        var mre = new AsyncManualResetEvent();

                        var _ = Subscription2.Run(async x =>
                        {
                            foreach (var item in x.Items)
                            {
                                con2Docs.Add(item.Id);
                            }

                            mre.Set();
                            await tcs.Task;
                        });

                        await mre.WaitAsync();

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
                    }, tcs);
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
                    var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                    db.SubscriptionStorage.TryGetRunningSubscriptionConnectionsState(long.Parse(id), out var subscriptionConnectionsState);
                    Assert.Equal(1, subscriptionConnectionsState.GetConnections().Count);
                    await AssertWaitForTrueAsync(() => Task.FromResult(con1Docs.Count + con2Docs.Count == 6 || con1Docs.Count + con2Docs.Count == 8), 6000);
                    await AssertNoLeftovers(store, id);
                }
            }
        }

        [Theory]
        [InlineData(SubscriptionOpeningStrategy.TakeOver, SubscriptionOpeningStrategy.Concurrent)]
        [InlineData(SubscriptionOpeningStrategy.Concurrent, SubscriptionOpeningStrategy.TakeOver)]
        public async Task CannotConnectInDifferentMode(SubscriptionOpeningStrategy strategy1, SubscriptionOpeningStrategy strategy2)
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                using var subscription1 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    Strategy = strategy1,
                });
                using var subscription2 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    Strategy = strategy2,
                });

                var mre1 = new AsyncManualResetEvent();

                subscription1.OnEstablishedSubscriptionConnection += mre1.Set;

                var t = subscription1.Run(x =>
                {

                });

                Assert.True(await mre1.WaitAsync(TimeSpan.FromSeconds(15)));
                mre1.Reset();

                await Assert.ThrowsAsync<SubscriptionInUseException>(() => subscription2.Run((_) => { }));
                await store.Subscriptions.DropSubscriptionWorkerAsync(subscription1);
                await Assert.ThrowsAsync<SubscriptionClosedException>(() => t);
            }
        }

        [Theory]
        [InlineData(SubscriptionOpeningStrategy.TakeOver, SubscriptionOpeningStrategy.Concurrent)]
        [InlineData(SubscriptionOpeningStrategy.Concurrent, SubscriptionOpeningStrategy.TakeOver)]
        public async Task CanDropAndConnectInDifferentMode(SubscriptionOpeningStrategy strategy1, SubscriptionOpeningStrategy strategy2)
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                using var subscription1 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    Strategy = strategy1,
                });
                using var subscription2 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    Strategy = strategy2,
                });

                var mre1 = new AsyncManualResetEvent();
                var mre2 = new AsyncManualResetEvent();

                subscription1.OnEstablishedSubscriptionConnection += mre1.Set;
                subscription2.OnEstablishedSubscriptionConnection += mre2.Set;

                var t = subscription1.Run(x =>
                {

                });

                Assert.True(await mre1.WaitAsync(TimeSpan.FromSeconds(15)));
                mre1.Reset();

                await store.Subscriptions.DropSubscriptionWorkerAsync(subscription1);
                await Assert.ThrowsAsync<SubscriptionClosedException>(() => t);

                t = subscription2.Run((_) => { });
                Assert.True(await mre2.WaitAsync(TimeSpan.FromSeconds(15)));
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

        [Fact]
        public async Task ShouldClearOldItemsFromResendListOnBatchProcessing()
        {
            using (var store = GetDocumentStore())
            {
                var collectionSize = 1000;
                using (var session = store.OpenSession())
                {

                    for (int i = 0; i < collectionSize; i++)
                    {
                        session.Store(new User() { Name = "E" }, $"user/{i}");
                    }

                    session.SaveChanges();
                }

                var mre = new AsyncManualResetEvent();
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = "from Users where Name != 'R'"
                });

                var count = 0;
                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1),
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    MaxDocsPerBatch = 64
                }))
                {
                    var t = subscription.Run(async x =>
                    {
                        using var s = x.OpenAsyncSession();
                        foreach (var item in x.Items)
                        {
                            item.Result.Name = "G";
                        }

                        await s.SaveChangesAsync();

                        foreach (var item in x.Items)
                        {
                            item.Result.Name = "R";
                        }

                        await s.SaveChangesAsync();

                        var res = Interlocked.Add(ref count, x.NumberOfItemsInBatch);
                        if (res >= collectionSize)
                        {
                            mre.Set();
                        }

                    });

                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(60)));

                    using (var session = store.OpenSession())
                    {
                        var u = session.Load<User>("user/1");
                        Assert.Equal("R", u.Name);
                    }

                    var executor = store.GetRequestExecutor();
                    using var _ = executor.ContextPool.AllocateOperationContext(out var ctx);
                    var cmd = new GetSubscriptionResendListCommand(store.Database, id);

                    var finalRes = await WaitForValueAsync(() =>
                    {
                        executor.Execute(cmd, ctx);
                        var res = cmd.Result.Results.First();

                        return res.ResendList.Count;
                    }, 0);

                    Assert.Equal(0, finalRes);
                }
            }
        }

        [Fact]
        public async Task ShouldClearSubscriptionInfoFromStorageAfterDatabaseDeletion()
        {
            DoNotReuseServer();
            const int expectedNumberOfDocsToResend = 7;

            string databaseName = GetDatabaseName();

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            await Backup.HoldBackupExecutionIfNeededAndInvoke(ts: null, async () =>
            {
                using (var store = GetDocumentStore(new Options { ModifyDatabaseName = _ => databaseName }))
                {
                    var subscriptionId = await store.Subscriptions.CreateAsync<User>();
                    await using var subscriptionWorker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionId)
                    {
                        Strategy = SubscriptionOpeningStrategy.Concurrent,
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(2),
                        MaxDocsPerBatch = expectedNumberOfDocsToResend
                    });

                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                            session.Store(new User { Name = $"UserNo{i}" });

                        session.SaveChanges();
                    }

                    _ = subscriptionWorker.Run(async x =>
                    {
                        await tcs.Task;
                    });

                    await AssertWaitForValueAsync(() =>
                    {
                        List<SubscriptionStorage.ResendItem> items;

                        using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                        using (context.OpenReadTransaction())
                            items = SubscriptionStorage.GetResendItemsForDatabase(context, store.Database).ToList();

                        return Task.FromResult(items.Count);
                    }, expectedNumberOfDocsToResend);
                }

                // Upon disposing of the store, the database gets deleted.
                // Then we recreate the database to ensure no leftover subscription data from the previous instance.
                using (var _ = GetDocumentStore(new Options { ModifyDatabaseName = _ => databaseName }))
                {
                    List<SubscriptionStorage.ResendItem> items;

                    using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                        items = SubscriptionStorage.GetResendItemsForDatabase(context, databaseName).ToList();

                    Assert.Equal(0, items.Count);
                }
            }, tcs);
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [InlineData(1)]
        [InlineData(3)]
        public async Task ConcurrentSubscriptionsShouldContinueProcessOnNewConnections(int count)
        {
            using (var store = GetDocumentStore())
            {
                var id = await store.Subscriptions.CreateAsync<User>();
                var docs = new HashSet<string>();

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), $"users/{i}");
                        session.SaveChanges();
                    }

                    var workers = new List<SubscriptionWorker<User>>();
                    for (int j = 0; j < count; j++)
                    {
                        workers.Add(store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                        {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1),
                            Strategy = SubscriptionOpeningStrategy.Concurrent,
                            MaxDocsPerBatch = 1
                        }));
                    }

                    try
                    {
                        foreach (var worker in workers)
                        {
                            var t = worker.Run(x =>
                            {
                                foreach (var item in x.Items)
                                {
                                    docs.Add(item.Id);
                                }
                            });
                        }

                        await AssertWaitForTrueAsync(() => Task.FromResult(docs.Count == i + 1), Convert.ToInt32(_reasonableWaitTime.TotalMilliseconds));
                        await AssertNoLeftovers(store, id);
                    }
                    finally
                    {
                        foreach (var w in workers)
                        {
                            await w.DisposeAsync();
                        }
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [InlineData(1)]
        [InlineData(3)]
        public async Task ConcurrentSubscriptionsShouldContinueProcessOnNewConnectionsAfterUpdate(int count)
        {
            using (var store = GetDocumentStore())
            {
                var id = await store.Subscriptions.CreateAsync<User>(options: new SubscriptionCreationOptions<User>()
                {
                    Filter = user => user.Age == 0
                });

                var docs = new HashSet<string>();

                for (int i = 0; i < 10; i++)
                {
                    if (i > 0)
                    {
                        await store.Subscriptions.UpdateAsync(options: new SubscriptionUpdateOptions()
                        {
                            Name = id,
                            Query = @$"declare function predicate() {{ return this.Age==={i} }}
from 'Users' as doc
where predicate.call(doc)"
                        });
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User()
                        {
                            Age = i
                        }, $"users/{i}");
                        session.SaveChanges();
                    }

                    var workers = new List<SubscriptionWorker<User>>();
                    for (int j = 0; j < count; j++)
                    {
                        workers.Add(store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                        {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1),
                            Strategy = SubscriptionOpeningStrategy.Concurrent,
                            MaxDocsPerBatch = 1
                        }));
                    }

                    try
                    {
                        foreach (var worker in workers)
                        {
                            var t = worker.Run(x =>
                            {
                                foreach (var item in x.Items)
                                {
                                    docs.Add(item.Id);
                                }
                            });
                        }

                        await AssertWaitForTrueAsync(() => Task.FromResult(docs.Count == i + 1), Convert.ToInt32(_reasonableWaitTime.TotalMilliseconds));
                        await AssertNoLeftovers(store, id);
                    }
                    finally
                    {
                        foreach (var w in workers)
                        {
                            await w.DisposeAsync();
                        }
                    }
                }

                var subs = await store.Subscriptions.GetSubscriptionsAsync(0, 1024);
                Assert.Equal(1, subs.Count);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task ConcurrentSubscriptionsShouldContinueProcessOnNewConnectionsAfterUpdate_AndDisposeWhileConnecting()
        {
            using (var store = GetDocumentStore())
            {
                var id = await store.Subscriptions.CreateAsync<User>(options: new SubscriptionCreationOptions<User>()
                {
                    Filter = user => user.Age == 0
                });

                var docs = new HashSet<string>();
                var workers = new List<SubscriptionWorker<User>>();
                try
                {
                    for (int i = 0; i < 10; i++)
                    {
                        if (i > 0)
                        {
                            // we update the subscription on each iteration
                            // so it will process only the new created document
                            await store.Subscriptions.UpdateAsync(options: new SubscriptionUpdateOptions()
                            {
                                Name = id,
                                Query = @$"declare function predicate() {{ return this.Age==={i} }}
from 'Users' as doc
where predicate.call(doc)"
                            });
                        }

                        using (var session = store.OpenSession())
                        {
                            session.Store(new User()
                            {
                                Age = i
                            }, $"users/{i}");
                            session.SaveChanges();
                        }

                        var w = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                        {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1),
                            Strategy = SubscriptionOpeningStrategy.Concurrent,
                            MaxDocsPerBatch = 1
                        });
                        workers.Add(w);

                        var t2 = w.Run(x =>
                        {
                            foreach (var item in x.Items)
                            {
                                docs.Add(item.Id);
                                Thread.Sleep(1000);
                            }
                        });

                        await AssertWaitForTrueAsync(() => Task.FromResult(docs.Count == i + 1), Convert.ToInt32(_reasonableWaitTime.TotalMilliseconds));
                        await AssertNoLeftovers(store, id);
                    }
                }
                finally
                {
                    foreach (var w in workers)
                    {
                        await w.DisposeAsync();
                    }
                }

                var subs = await store.Subscriptions.GetSubscriptionsAsync(0, 1024);
                Assert.Equal(1, subs.Count);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task ConcurrentSubscriptionsShouldProcessWhen_2_ConnectionsAreSubscribingConcurrently()
        {
            using (var store = GetDocumentStore())
            {
                var id = await store.Subscriptions.CreateAsync<User>();

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var testingStuff = db.ForTestingPurposesOnly();
                var connectionsCount = 0L;
                using (testingStuff.CallDuringWaitForSubscribe(connections =>
                {
                    Interlocked.Increment(ref connectionsCount);

                    while (Interlocked.Read(ref connectionsCount) < 2)
                    {
                        Thread.Sleep(111);
                    }
                }))
                {
                    await using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                        Strategy = SubscriptionOpeningStrategy.Concurrent,
                        MaxDocsPerBatch = 2
                    }))
                    await using (var secondSubscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
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

                        await AssertWaitForTrueAsync(() => Task.FromResult(con1Docs.Count + con2Docs.Count == 6), Convert.ToInt32(_reasonableWaitTime.TotalMilliseconds));
                        await AssertNoLeftovers(store, id);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task ProcessOnResponsibleNodeThenOnDifferentNodeThenBackOnResponsible()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);

            using (var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 3,
                Server = cluster.Leader,
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "EGOR" }, "user/1");
                    session.SaveChanges();
                }

                var database = store.Database;
                var id = await store.Subscriptions.CreateAsync<User>();

                //responsible node processes doc
                HashSet<string> docs = await RunSubscriptionWorkerAndProcessOneDocumentAsync(store, id);

                var node1 = string.Empty;
                Assert.True(await WaitForValueAsync(async () =>
                {
                    foreach (var node in cluster.Nodes)
                    {
                        var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            if (db.SubscriptionStorage.TryGetRunningSubscriptionConnectionsState(long.Parse(id), out var subscriptionConnectionsState))
                            {
                                node1 = node.ServerStore.NodeTag;
                                return true;
                            }
                        }
                    }

                    return false;
                }, true), "1st doc processed");

                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "EGR" }, "user/2");
                    session.SaveChanges();
                }

                // move responsible node to rehab
                var responsibleNode = cluster.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == node1);
                Assert.NotNull(responsibleNode);
                responsibleNode.CpuCreditsBalance.BackgroundTasksAlertRaised.Raise();
                var rehabs = await WaitForValueAsync(async () => await GetRehabCount(store, store.Database), 1);
                Assert.Equal(1, rehabs);

                // another node processes doc
                docs.UnionWith(await RunSubscriptionWorkerAndProcessOneDocumentAsync(store, id));

                Assert.Equal(2, await WaitForValueAsync(async () =>
                {
                    var i = 0;
                    foreach (var node in cluster.Nodes)
                    {
                        var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            if (db.SubscriptionStorage.TryGetRunningSubscriptionConnectionsState(long.Parse(id), out _))
                            {
                                i++;
                            }
                        }
                    }

                    return i;
                }, 2));

                // move responsible node back from rehab
                responsibleNode.CpuCreditsBalance.BackgroundTasksAlertRaised.Lower();
                var members = await WaitForValueAsync(async () => await GetMembersCount(store, store.Database), 3);
                Assert.Equal(3, members);

                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "EGOOOOOOR" }, "user/3");
                    session.SaveChanges();
                }

                Assert.Equal(2, docs.Count);

                // responsible node processes doc
                docs.UnionWith(await RunSubscriptionWorkerAndProcessOneDocumentAsync(store, id));

                Assert.Equal(3, docs.Count);
            }
        }

        private static async Task<HashSet<string>> RunSubscriptionWorkerAndProcessOneDocumentAsync(DocumentStore store, string id)
        {
            var docs = new HashSet<string>();
            await using var worker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
            {
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                Strategy = SubscriptionOpeningStrategy.Concurrent,
                MaxDocsPerBatch = 1
            });

            worker.AfterAcknowledgment += batch =>
            {
                foreach (var item in batch.Items)
                {
                    docs.Add(item.Id);
                }

                return Task.CompletedTask;
            };

            var t = worker.Run(x => { });

            Assert.Equal(1, await WaitForValueAsync(() => docs.Count, 1));
            return docs;
        }

        private class GetSubscriptionResendListCommand : RavenCommand<ResendListResults>
        {
            private readonly string _database;
            private readonly string _name;

            public GetSubscriptionResendListCommand(string database, string name)
            {
                _database = database;
                _name = name;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_database}/debug/subscriptions/resend?name={_name}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                {
                    Result = null;
                    return;
                }

                var deserialize = JsonDeserializationBase.GenerateJsonDeserializationRoutine<ResendListResults>();
                Result = deserialize.Invoke(response);
            }

            public override bool IsReadRequest => true;
        }

        private class ResendListResults
        {
            public List<ResendListResult> Results;
        }

        private class ResendListResult
        {
            public string SubscriptionName;
            public long SubscriptionId;
            public List<long> Active;
            public List<SubscriptionStorage.ResendItem> ResendList;
        }

        private class User
        {
            public string Name;
            public int Age;
        }
    }
}
