using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Cluster
{
    public class SubscriptionsWithReshardingTests : ClusterTestBase
    {
        public SubscriptionsWithReshardingTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task ContinueSubscriptionAfterResharding()
        {
            using var store = Sharding.GetDocumentStore();
            await SubscriptionWithResharding(store);
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocumentOnce()
        {
            using var store = Sharding.GetDocumentStore();
            using (var session = store.OpenSession())
            {
                session.Store(new User(), "users/1-A");
                session.SaveChanges();
            }

            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");

            var id = await store.Subscriptions.CreateAsync<User>();
            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 5, 
                             TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
                         }))
            {
                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (users.Add(item.Id) == false)
                        {
                            throw new SubscriberErrorException($"Got exact same {item.Id} twice");
                        }
                    }
                });


                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(5));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocumentOnce2()
        {
            using var store = Sharding.GetDocumentStore();
            var numberOfDocs = 100;

            var writes = Task.Run(() =>
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                    }, "users/1-A");
                    session.SaveChanges();
                }

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                        }, $"num-{i}$users/1-A");
                        session.SaveChanges();
                    }
                }
            });
           
            var sub = await store.Subscriptions.CreateAsync<User>();
            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sub)
                         {
                             MaxDocsPerBatch = 5, 
                             TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250),
                         }))
            {
                var mre = new AsyncManualResetEvent();
                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (users.Add(item.Id) == false)
                        {
                            throw new SubscriberErrorException($"Got exact same {item.Id} twice");
                        }

                        if (users.Count == numberOfDocs+1)
                        {
                            mre.Set();
                        }
                    }
                });
                
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");

                await writes;

                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(15));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                if (await mre.WaitAsync(TimeSpan.FromSeconds(3)) == false)
                {
                    await subscription.DisposeAsync(true);
                    WaitForUserToContinueTheTest(store, debug:false);
                }

                for (int i = 0; i < numberOfDocs; i++)
                {
                    var id = $"num-{i}$users/1-A";
                    Assert.True(users.Contains(id),$"{id} is missing");
                }
                
                Assert.True(users.Contains("users/1-A"),"users/1-A is missing");
                Assert.Equal(numberOfDocs + 1, users.Count);

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, sub);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocumentsWithFilteringAndModifications()
        {
            using var store = Sharding.GetDocumentStore();
            var docsCount = 100;
            var writes = Task.Run(async () =>
            {
                for (int j = 0; j < 10; j++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await AddOrUpdateUserAsync(session, "users/1-A");
                        await session.SaveChangesAsync();
                    }

                    for (int i = 3; i < docsCount; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await AddOrUpdateUserAsync(session, $"num-{i}$users/1-A");
                            await AddOrUpdateUserAsync(session, $"users/{i}-A");
                            await session.SaveChangesAsync();
                        }
                    }
                }
            });

            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 5, 
                             TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
                         }))
            {
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var timeoutEvent = new TimeoutEvent(TimeSpan.FromSeconds(5), "foo");
                timeoutEvent.Start(tcs.SetResult);

                var t = subscription.Run(batch =>
                {
                    timeoutEvent.Defer("Foo");
                    foreach (var item in batch.Items)
                    {
                        // Console.WriteLine($"Subscription got {item.Id} with age:{item.Result.Age}, cv: {item.ChangeVector}");

                        if (users.TryGetValue(item.Id, out var age))
                        {
                            if (Math.Abs(age) >= Math.Abs(item.Result.Age))
                            {
                                Debug.Assert(false, $"Got an outdated user {item.Id}, existing: {age}, received: {item.Result.Age}");
                                throw new InvalidOperationException($"Got an outdated user {item.Id}, existing: {age}, received: {item.Result.Age}");
                            }
                        }

                        users[item.Id] = item.Result.Age;
                    }
                });
                
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");

                await writes;

                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(5));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

                    await WaitAndAssertForValueAsync(() => session.Query<User>().CountAsync(), (docsCount - 3) * 2 + 1);

                    var usersByQuery = await session.Query<User>().Where(u => u.Age > 0).ToListAsync();
                    foreach (var user in usersByQuery)
                    {
                        Assert.True(users.TryGetValue(user.Id, out var age), $"Missing {user.Id} from subscription");
                        Assert.True(age == user.Age, $"From sub:{age}, from shard: {user.Age} for {user.Id} cv:{session.Advanced.GetChangeVectorFor(user)}");
                        users.Remove(user.Id);
                    }
                }
                
                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocumentsWithFilteringAndModifications2()
        {
            using var store = Sharding.GetDocumentStore();
            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            
            await CreateItems(store, 0, 2);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 2, 4);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 4, 6);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await ProcessSubscription(store, id, users);
            await CreateItems(store, 6, 7);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 7, 8);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 9, 10);
            await ProcessSubscription(store, id, users);


            using (var session = store.OpenAsyncSession())
            {
                var total = await session.Query<User>().CountAsync();
                Assert.Equal(195, total);

                var usersByQuery = await session.Query<User>().Where(u => u.Age > 0).ToListAsync();
                foreach (var user in usersByQuery)
                {
                    Assert.True(users.TryGetValue(user.Id, out var age), $"Missing {user.Id} from subscription");
                    Assert.True(age == user.Age, $"From sub:{age}, from shard: {user.Age} for {user.Id} cv:{session.Advanced.GetChangeVectorFor(user)}");
                    users.Remove(user.Id);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocumentsWithFilteringAndModifications3()
        {
            using var store = Sharding.GetDocumentStore();
            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            
            var t = ProcessSubscription(store, id, users, timoutSec: 15);
            await CreateItems(store, 0, 2);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 2, 4);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 4, 6);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 6, 7);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 7, 8);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 9, 10);


            await t;

            using (var session = store.OpenAsyncSession())
            {
                var total = await session.Query<User>().CountAsync();
                Assert.Equal(195, total);

                var usersByQuery = await session.Query<User>().Where(u => u.Age > 0).ToListAsync();
                foreach (var user in usersByQuery)
                {
                    Assert.True(users.TryGetValue(user.Id, out var age), $"Missing {user.Id} from subscription");
                    Assert.True(age == user.Age, $"From sub:{age}, from shard: {user.Age} for {user.Id} cv:{session.Advanced.GetChangeVectorFor(user)}");
                    users.Remove(user.Id);
                }
            }
        }

        private async Task ProcessSubscription(IDocumentStore store, string id, Dictionary<string, int> users, int timoutSec = 5)
        {
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 5, 
                             TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250),
                             // CloseWhenNoDocsLeft = true
                         }))
            {
                try
                {
                    var t = subscription.Run(batch =>
                    {
                        foreach (var item in batch.Items)
                        {
                            if (users.TryGetValue(item.Id, out var age))
                            {
                                if (Math.Abs(age) >= Math.Abs(item.Result.Age))
                                {
                                    throw new InvalidOperationException($"Got an outdated user {item.Id}, existing: {age}, received: {item.Result.Age}");
                                }
                            }

                            users[item.Id] = item.Result.Age;
                        }
                    });

                    await t.WaitAsync(TimeSpan.FromSeconds(timoutSec));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }
    

        private static async Task CreateItems(IDocumentStore store, int from, int to)
        {
            for (int j = from; j < to; j++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await AddOrUpdateUserAsync(session, "users/1-A");
                    await session.SaveChangesAsync();
                }

                for (int i = 3; i < 100; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await AddOrUpdateUserAsync(session, $"num-{i}$users/1-A");
                        await AddOrUpdateUserAsync(session, $"users/{i}-A");
                        await session.SaveChangesAsync();
                    }
                }
            }
        }

        private static async Task AddOrUpdateUserAsync(IAsyncDocumentSession session, string id)
        {
            var current = await session.LoadAsync<User>(id);
            if (current == null)
            {
                current = new User();
                var age = Random.Shared.Next(1024);
                current.Age = age % 2 == 0 ? -1 : 1;
                await session.StoreAsync(current, id);
                return;
            }

            Assert.True(current.Age != 0);

            if (current.Age > 0)
                current.Age++;
            else
                current.Age--;

            current.Age *= -1;
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ContinueSubscriptionAfterReshardingInACluster()
        {
            var cluster = await CreateRaftCluster(5, watcherCluster: true);
            using var store = Sharding.GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            });

            await SubscriptionWithResharding(store);
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Need to handle resharding failover")]
        public async Task ContinueSubscriptionAfterReshardingInAClusterWithFailover()
        {
            var cluster = await CreateRaftCluster(5, watcherCluster: true, shouldRunInMemory: false);
            using var store = Sharding.GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            });

            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var t = ProcessSubscription(store, id, users, timoutSec: 15);
            var sync = new AsyncManualResetEvent();
            var sync2 = new AsyncManualResetEvent();

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(3)))
            {
                var fail = Task.Run(async () =>
                {
                    try
                    {
                        while (cts.IsCancellationRequested == false)
                        {
                            var position = Random.Shared.Next(0, 5);
                            var node = cluster.Nodes[position];
                            if (node.ServerStore.IsLeader())
                                continue;

                            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
                            await Cluster.WaitForNodeToBeRehabAsync(store, node.ServerStore.NodeTag, token: cts.Token);
                            
                            sync.Set();
                            await sync2.WaitAsync(cts.Token);
                            sync2.Reset();

                            cluster.Nodes[position] = await ReviveNodeAsync(result);
                            await Cluster.WaitForAllNodesToBeMembersAsync(store, token: cts.Token);
                            
                            sync.Set();
                            await sync2.WaitAsync(cts.Token);
                            sync2.Reset();
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // shutdown
                    }
                });

                try
                {
                    await CreateItems(store, 0, 2);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", cluster.Nodes);
                    
                    await sync.WaitAsync(cts.Token);
                    sync.Reset();
                    sync2.Set();

                    await CreateItems(store, 2, 4);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", cluster.Nodes);

                    await sync.WaitAsync(cts.Token);
                    sync.Reset();
                    sync2.Set();

                    await CreateItems(store, 4, 6);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", cluster.Nodes);

                    await sync.WaitAsync(cts.Token);
                    sync.Reset();
                    sync2.Set();

                    await CreateItems(store, 6, 7);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", cluster.Nodes);

                    await sync.WaitAsync(cts.Token);
                    sync.Reset();
                    sync2.Set();

                    await CreateItems(store, 7, 8);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", cluster.Nodes);

                    await sync.WaitAsync(cts.Token);
                    sync.Reset();
                    sync2.Set();

                    await CreateItems(store, 9, 10);


                    await t;

                    using (var session = store.OpenAsyncSession())
                    {
                        var total = await session.Query<User>().CountAsync();
                        Assert.Equal(195, total);

                        var usersByQuery = await session.Query<User>().Where(u => u.Age > 0).ToListAsync();
                        foreach (var user in usersByQuery)
                        {
                            Assert.True(users.TryGetValue(user.Id, out var age), $"Missing {user.Id} from subscription");
                            Assert.True(age == user.Age, $"From sub:{age}, from shard: {user.Age} for {user.Id} cv:{session.Advanced.GetChangeVectorFor(user)}");
                            users.Remove(user.Id);
                        }
                    }
                }
                finally
                {
                   cts.Cancel();
                   await fail;
                }
            }

            await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
        }

        private async Task SubscriptionWithResharding(IDocumentStore store)
        {
            var id = await store.Subscriptions.CreateAsync<User>();
            var users = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var mre = new ManualResetEvent(false);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 5, 
                             TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
                         }))
            {
                subscription.AfterAcknowledgment += batch =>
                {
                    mre.Set();
                    return Task.CompletedTask;
                };

                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        users.TryAdd(item.Id, new HashSet<string>(StringComparer.Ordinal));
                        var cv = users[item.Id];

                        if (cv.Add(item.ChangeVector) == false)
                        {
                            throw new SubscriberErrorException($"Got exact same {item.Id} twice");
                        }
                    }
                });


                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "foo$users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User(), NextId);
                    }

                    session.Store(new User(), "foo$users/8-A");

                    session.SaveChanges();
                }

                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(5));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                mre.Reset();

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 1);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 1);

                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");

                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(5));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "bar$users/1-A");
                    session.Store(new User(), "users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User(), NextId);
                    }

                    session.Store(new User(), "bar$users/8-A");
                    session.Store(new User(), "users/8-A");

                    session.SaveChanges();
                }

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 2);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 2);
                
                Assert.True(mre.WaitOne(TimeSpan.FromSeconds(5)));
                mre.Reset();

                await Sharding.Resharding.MoveShardForId(store, "users/8-A");

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "baz$users/1-A");
                    session.Store(new User(), "users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User(), NextId);
                    }

                    session.Store(new User(), "baz$users/8-A");
                    session.Store(new User(), "users/8-A");


                    session.SaveChanges();
                }

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 3);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 3);
                await WaitForValueAsync(() => users.Count, 66);

                var expected = new HashSet<string>();
                for (int i = 1; i < 61; i++)
                {
                    var u = $"users/{i}-A";
                    expected.Add(u);
                }

                expected.Add("foo$users/1-A");
                expected.Add("bar$users/1-A");
                expected.Add("baz$users/1-A");
                expected.Add("foo$users/8-A");
                expected.Add("bar$users/8-A");
                expected.Add("baz$users/8-A");

                foreach (var user in users)
                {
                    expected.Remove(user.Key);
                }

                var config = await Sharding.GetShardingConfigurationAsync(store);
                Assert.True(expected.Count == 0,
                    $"Missing {string.Join(Environment.NewLine, expected.Select(e => $"{e} (shard: {ShardHelper.GetShardNumber(config.BucketRanges, e)})"))}");

                
                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }

        private int _current;
        private string NextId => $"users/{Interlocked.Increment(ref _current)}-A";
    }
}
