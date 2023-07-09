using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;
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

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanProcessSubscriptionDuringReshardingSameBucketAndWriting()
        {
            using var store = Sharding.GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new User { }, "users/1-A");
                session.SaveChanges();
            }

            var adding = true;
            int? cc = 0;
            var x = 1_000_000;
            var list1 = new List<(string, string)>();
            var writes = Task.Run(() =>
            {
                var i = 0;
                while (adding)
                {
                    using (var session = store.OpenSession())
                    {
                        var id1 = $"num-{i++}$users/1-A";
                        var id2 = $"users/{--x}-A";
                        var u1 = new User { };
                        var u2 = new User { };
                        session.Store(u1, id1);
                        session.Store(u2, id2);

                        session.SaveChanges();
                        var cv1 = session.Advanced.GetChangeVectorFor(u1);
                        var cv2 = session.Advanced.GetChangeVectorFor(u2);
                        list1.Add((id1, cv1));
                        list1.Add((id2, cv2));
                    }

                    cc += 2;
                    Thread.Sleep(8);
                }
            });

            var sub = await store.Subscriptions.CreateAsync<User>();
            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var usersList = new List<(string, string)>();
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sub)
                         {
                             MaxDocsPerBatch = 6, TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250),
                         }))
            {
                var twiceIds = new List<(string, string)>();
                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (users.Add(item.Id) == false)
                        {
                            twiceIds.Add((item.Id, item.ChangeVector));
                        }
                        else
                        {
                            usersList.Add((item.Id, item.ChangeVector));
                        }
                    }
                });
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                adding = false;
                await writes;

                var val = WaitForValue(() =>
                {
                    if (cc == null)
                        return false;
                    if (cc + 1 != users.Count)
                    {
                        return false;
                    }

                    return true;
                }, true, timeout: 5 * 60_000, interval: 1000);

                Assert.True(val, $"Added docs: {cc} / Processed users: {users.Count}"
                                 + $"{Environment.NewLine}-----Missing({list1.Except(usersList).ToList().Count}): " + string.Join(",", list1.Except(usersList).ToList())
                                 + $"{Environment.NewLine}-----Duplicates({twiceIds.Count}): " + string.Join(",", twiceIds));
                Assert.Equal(cc + 1, users.Count);

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, sub);
            }
        }

        [RavenTheory(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        [RavenData(1, DatabaseMode = RavenDatabaseMode.Sharded)]
        [RavenData(2, DatabaseMode = RavenDatabaseMode.Sharded)]
        [RavenData(3, DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task CanContinueSubscription_WhenAfterResharding_BatchGotFailover(Options options, int rounds)
        {
            using var server = GetNewServer(new ServerCreationOptions() { RunInMemory = false });
            var servers =
                new List<RavenServer>() { server };
            options.RunInMemory = false;
            options.Server = server;
            using var store = Sharding.GetDocumentStore(options);
            var list1 = new List<(string, string)>();

            using (var session = store.OpenSession())
            {
                var id1 = "users/1-A";
                var u1 = new User { };
                session.Store(u1, id1);
                session.SaveChanges();
                var cv1 = session.Advanced.GetChangeVectorFor(u1);
                list1.Add((id1, cv1));
            }

            var conf = await Sharding.GetShardingConfigurationAsync(store);
            int shardNumber;
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
                shardNumber = ShardHelper.GetShardNumberFor(conf, allocator, "users/1-A");


            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var usersList = new List<(string, string)>();

            var id = await store.Subscriptions.CreateAsync<User>();
            var mre = new ManualResetEventSlim(false);
            var mre2 = new ManualResetEventSlim(false);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 1, TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
                         }))
            {
                var twiceIds = new List<(string, string)>();
                subscription.AfterAcknowledgment += batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (users.Add(item.Id) == false)
                        {
                            twiceIds.Add((item.Id, item.ChangeVector));
                        }
                        else
                        {
                            usersList.Add((item.Id, item.ChangeVector));
                        }
                    }

                    return Task.CompletedTask;
                };

                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (item.Id == "users/1-A")
                        {
                            mre.Set();
                            mre2.Wait(TimeSpan.FromSeconds(60));
                        }
                    }
                });

                mre.Wait(TimeSpan.FromSeconds(60));

                for (int i = 0; i < rounds; i++)
                {
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: servers);
                }

                await foreach (var db in Sharding.GetShardsDocumentDatabaseInstancesFor(store, servers))
                {
                    if (db.ShardNumber == shardNumber)
                    {
                        await server.ServerStore.DatabasesLandlord.RestartDatabaseAsync(db.Name);
                        break;
                    }
                }

                mre2.Set();
                var val = WaitForValue(() =>
                {
                    if (1 != users.Count)
                    {
                        return false;
                    }

                    return true;
                }, true, timeout: 60_000, interval: 333);

                Assert.True(val, $"Added docs: 1 / Processed users: {users.Count}"
                                 + $"{Environment.NewLine}-----Missing({list1.Except(usersList).ToList().Count}): " + string.Join(",", list1.Except(usersList).ToList())
                                 + $"{Environment.NewLine}-----Duplicates({twiceIds.Count}): " + string.Join(",", twiceIds));
                Assert.Equal(1, users.Count);

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id, servers);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task CanContinueSubscriptionAfterReshardingDocumentFromSameShard()
        {
            using var store = Sharding.GetDocumentStore();
            var conf = await Sharding.GetShardingConfigurationAsync(store);
            int shardNumber1;
            int shardNumber2;
            var tuple = GetIdsOnSameShardAndDifferentBuckets(conf);
            shardNumber1 = tuple.Tuple1.ShardNumber;
            var id1 = tuple.Tuple1.Id;
            var id2 = tuple.Tuple2.Id;
            shardNumber2 = tuple.Tuple2.ShardNumber;

            var idsList = new List<(string, int, int)>() { tuple.Tuple1, tuple.Tuple2 };
            using (var session = store.OpenSession())
            {
                session.Store(new User(), id1); //A:1
                session.Store(new User(), id2); //A:2
                session.SaveChanges();
            }

            await Sharding.Resharding.MoveShardForId(store, id2);
            // $0: A:1-db1, $1: A:1-db2, A:2-db1

            var id = await store.Subscriptions.CreateAsync<User>();

            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var usersList = new List<(string, string)>();

            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 1, TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(777)
                         }))
            {
                var db = await Sharding.GetShardsDocumentDatabaseInstancesFor(store).FirstOrDefaultAsync(x => x.ShardNumber == shardNumber1);
                Assert.NotNull(db);
                var testingStuff = db.ForTestingPurposesOnly();
                using var disposable = testingStuff.CallAfterRegisterSubscriptionConnection(_ =>
                {
                    // make sure $0 doesn't process anything
                    Thread.Sleep(1000);
                    throw new SubscriptionDoesNotBelongToNodeException("DROPPED BY TEST");
                });

                var twiceIds = new List<(string, string)>();
                subscription.AfterAcknowledgment += batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (users.Add(item.Id) == false)
                        {
                            twiceIds.Add((item.Id, item.ChangeVector));
                        }
                        else
                        {
                            usersList.Add((item.Id, item.ChangeVector));
                        }
                    }

                    return Task.CompletedTask;
                };
                var t = subscription.Run(batch =>
                {
                });

                Assert.True(WaitForValue(() => users.Contains(id2), true, timeout: 60_000, interval: 333), "users.Contains(id2)");
                // $0 will start to process
                disposable.Dispose();
                Assert.True(WaitForValue(() => users.Contains(id1), true, timeout: 60_000, interval: 333), "users.Contains(id1)");

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task CanContinueSubscriptionAfterReshardingDocumentFromSameShard2()
        {
            using var store = Sharding.GetDocumentStore();
            var conf = await Sharding.GetShardingConfigurationAsync(store);
            int shardNumber1;
            int shardNumber2;
            var tuple = GetIdsOnSameShardAndDifferentBuckets(conf);
            shardNumber1 = tuple.Tuple1.ShardNumber;
            var id1 = tuple.Tuple1.Id;
            var id2 = tuple.Tuple2.Id;
            shardNumber2 = tuple.Tuple2.ShardNumber;

            var idsList = new List<(string, int, int)>() { tuple.Tuple1, tuple.Tuple2 };
            using (var session = store.OpenSession())
            {
                session.Store(new User(), id1); //A:1
                session.Store(new User(), id2); //A:2
                session.SaveChanges();
            }

            await Sharding.Resharding.MoveShardForId(store, id1);
            // $0: A:2-db1, $1: A:1-db2, A:1-db1

            var id = await store.Subscriptions.CreateAsync<User>();

            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var usersList = new List<(string, string)>();

            var mre0 = new ManualResetEventSlim(false);
            var mre = new ManualResetEventSlim(false);
            var mre2 = new ManualResetEventSlim(false);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 1, TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(777)
                         }))
            {
                var db = await Sharding.GetShardsDocumentDatabaseInstancesFor(store).FirstOrDefaultAsync(x => x.ShardNumber == shardNumber2);
                Assert.NotNull(db);
                var testingStuff = db.ForTestingPurposesOnly();
                using var disposable = testingStuff.CallAfterRegisterSubscriptionConnection(_ =>
                {
                    // make sure $0 doesn't process anything
                    Thread.Sleep(1000);
                    throw new SubscriptionDoesNotBelongToNodeException("DROPPED BY TEST");
                });

                var twiceIds = new List<(string, string)>();
                subscription.AfterAcknowledgment += batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (users.Add(item.Id) == false)
                        {
                            twiceIds.Add((item.Id, item.ChangeVector));
                        }
                        else
                        {
                            usersList.Add((item.Id, item.ChangeVector));
                        }
                    }

                    return Task.CompletedTask;
                };
                var t = subscription.Run(batch =>
                {
                });
                Assert.True(WaitForValue(() => users.Contains(id1), true, timeout: 60_000, interval: 333), "users.Contains(id1)");
                // $1 will start to process
                disposable.Dispose();
                Assert.True(WaitForValue(() => users.Contains(id2), true, timeout: 60_000, interval: 333), "users.Contains(id2)");

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task ContinueSubscriptionAfterReshardingAndFailover()
        {
            /*
            shard1 send some doc
            shard3 send some another doc
            this doc get resharded (stays in resend list)
            shard1 got it, and start processing 
                */
            using var store = Sharding.GetDocumentStore();
            var conf = await Sharding.GetShardingConfigurationAsync(store);
            int shardNumber1;
            int shardNumber2;
            var tuple = GetIdsOnDifferentShards(conf);
            shardNumber1 = tuple.Tuple1.ShardNumber;
            var id1 = tuple.Tuple1.Id;
            var id2 = tuple.Tuple2.Id;
            shardNumber2 = tuple.Tuple2.ShardNumber;
            var idsList = new List<(string, int)>() { tuple.Tuple1, tuple.Tuple2 };
            using (var session = store.OpenSession())
            {
                session.Store(new User(), id1);
                session.Store(new User(), id2);
                session.SaveChanges();
            }


            var id = await store.Subscriptions.CreateAsync<User>();
            var users = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var mre0 = new ManualResetEventSlim(false);
            var mre = new ManualResetEventSlim(false);
            var mre2 = new ManualResetEventSlim(false);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 1, TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
                         }))
            {
                var proceesed = string.Empty;
                var batchNumber = 0;

                var proceesedList = new List<string>();
                subscription.AfterAcknowledgment += batch =>
                {
                    mre.Set();
                    batchNumber++;
                    proceesed = batch.Items.First().Id;
                    proceesedList.Add(proceesed);
                    return Task.CompletedTask;
                };
                var t = subscription.Run(batch =>
                {
                    if (batchNumber > 0)
                    {
                        mre0.Set();
                        mre2.Wait(TimeSpan.FromSeconds(60));
                    }
                });

                mre.Wait(TimeSpan.FromSeconds(60)); // process 1 doc from shard 1
                var notProcessed = idsList.First(x => x.Item1 != proceesed);
                mre0.Wait(TimeSpan.FromSeconds(60)); // hold 1 doc from shard 2

                await Sharding.Resharding.MoveShardForId(store, notProcessed.Item1);
                List<ShardedDocumentDatabase> shards = new List<ShardedDocumentDatabase>();
                await foreach (var db in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
                {
                    if (db.ShardNumber == notProcessed.Item2)
                    {
                        db.Dispose();
                        continue;
                    }

                    shards.Add(db);
                }

                mre2.Set();
                var count = await WaitForValueAsync(() => proceesedList.Count, 2, timeout: 60_000);
                Assert.Equal(2, count);
                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id, shards: shards);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task ContinueSubscriptionAfterResharding()
        {
            using var store = Sharding.GetDocumentStore();
            await SubscriptionWithResharding(store);
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
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
                             MaxDocsPerBatch = 5, TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
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

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task GetDocumentOnce2()
        {
            using var store = Sharding.GetDocumentStore();
            var numberOfDocs = 100;

            using (var session = store.OpenSession())
            {
                session.Store(new User { }, "users/1-A");
                session.SaveChanges();
            }

            var writes = Task.Run(() =>
            {
                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { }, $"num-{i}$users/1-A");
                        session.SaveChanges();
                    }
                }
            });

            var sub = await store.Subscriptions.CreateAsync<User>();
            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sub)
                         {
                             MaxDocsPerBatch = 5, TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250),
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

                        if (users.Count == numberOfDocs + 1)
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
                }

                for (int i = 0; i < numberOfDocs; i++)
                {
                    var id = $"num-{i}$users/1-A";
                    Assert.True(users.Contains(id), $"{id} is missing");
                }

                Assert.True(users.Contains("users/1-A"), "users/1-A is missing");
                Assert.Equal(numberOfDocs + 1, users.Count);

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, sub);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task GetDocumentsWithFilteringAndModifications()
        {
            using var store = Sharding.GetDocumentStore();
            var docsCount = 100;
            using (var session = store.OpenAsyncSession())
            {
                await AddOrUpdateUserAsync(session, "users/1-A", update: false);
                await session.SaveChangesAsync();
            }

            var writes = Task.Run(async () =>
            {
                for (int j = 0; j < 10; j++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await AddOrUpdateUserAsync(session, "users/1-A", update: false);
                        await session.SaveChangesAsync();
                    }

                    for (int i = 3; i < docsCount; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await AddOrUpdateUserAsync(session, $"num-{i}$users/1-A", update: false);
                            await AddOrUpdateUserAsync(session, $"users/{i}-A", update: false);
                            await session.SaveChangesAsync();
                        }
                    }
                }
            });

            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 5, TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
                         }))
            {
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var timeoutEvent = new TimeoutEvent(TimeSpan.FromSeconds(15), "foo");
                timeoutEvent.Start(tcs.SetResult);

                var t = subscription.Run(batch =>
                {
                    timeoutEvent.Defer("Foo");
                    foreach (var item in batch.Items)
                    {
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

                await tcs.Task.WaitAsync(TimeSpan.FromSeconds(60));

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

                    await WaitAndAssertForValueAsync(() => session.Query<User>().CountAsync(), (docsCount - 3) * 2 + 1, timeout: 30_000);

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

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task GetDocumentsWithFilteringAndModifications2()
        {
            using var store = Sharding.GetDocumentStore();
            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            await CreateItems(store, 0, 2);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 2, 4, update: true);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 4, 6, update: true);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 6, 7, update: true);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 7, 8, update: true);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 9, 10, update: true);
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

            await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task GetDocumentsWithFilteringAndModifications3()
        {
            using var store = Sharding.GetDocumentStore();
            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            var t = Task.Run(async () => await ProcessSubscription(store, id, users, timoutSec: 15));
            await CreateItems(store, 0, 2);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 2, 4, update: true);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 4, 6, update: true);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 6, 7, update: true);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 7, 8, update: true);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 9, 10, update: true);

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

            await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
        }

        private async Task ProcessSubscription(IDocumentStore store, string id, Dictionary<string, int> users, int timoutSec = 15)
        {
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 5, TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250),
                             // CloseWhenNoDocsLeft = true
                         }))
            {
                var mre = new ManualResetEventSlim(false);

                long timeout = timoutSec * 1000;
                subscription.AfterAcknowledgment += _ =>
                {
                    mre.Set();
                    Interlocked.Exchange(ref timeout, timoutSec * 1000);
                    return Task.CompletedTask;
                };
                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (users.TryGetValue(item.Id, out var age))
                        {
                            if (Math.Abs(age) > Math.Abs(item.Result.Age))
                            {
                                Console.WriteLine(
                                    $"InvalidOperationException($\"Got an outdated user {item.Id}, existing: {age} ({Math.Abs(age)}), received: {item.Result.Age}({Math.Abs(item.Result.Age)})\")");
                            }
                        }

                        users[item.Id] = item.Result.Age;
                    }
                });

                Assert.True(mre.Wait(TimeSpan.FromSeconds(60)), "mre.Wait(TimeSpan.FromSeconds(60))");
                while (Interlocked.Read(ref timeout) >= 0)
                {
                    await Task.Delay(500);
                    Interlocked.Add(ref timeout, -500);
                }

                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(1));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }
            }
        }

        private static async Task<int> CreateItems(IDocumentStore store, int from, int to, bool update = false)
        {
            var added = 0;
            try
            {
                for (int j = from; j < to; j++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        if (await AddOrUpdateUserAsync(session, "users/1-A", update))
                            added++;
                        await session.SaveChangesAsync();
                    }

                    for (int i = 3; i < 100; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            if (await AddOrUpdateUserAsync(session, $"num-{i}$users/1-A", update))
                                added++;

                            if (await AddOrUpdateUserAsync(session, $"users/{i}-A", update))
                                added++;
                            await session.SaveChangesAsync();
                        }
                    }
                }
            }
            catch (ShardMismatchException)
            {
                // expected
            }

            return added;
        }

        private static async Task<bool> AddOrUpdateUserAsync(IAsyncDocumentSession session, string id, bool update)
        {
            var current = await session.LoadAsync<User>(id);
            if (update && current == null)
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel,
                    DevelopmentHelper.Severity.Normal,
                    "Remove this if clause, after this is done: https://issues.hibernatingrhinos.com/issue/RavenDB-20818");

                var sp = Stopwatch.StartNew();
                while (sp.ElapsedMilliseconds <= 30_000)
                {
                    using var s = session.Advanced.DocumentStore.OpenAsyncSession();
                    Console.WriteLine($"current == null (id: {id})");
                    current = await s.LoadAsync<User>(id);
                    Thread.Sleep(1000);

                    if (current != null)
                    {
                        Assert.True(current.Age != 0);

                        if (current.Age > 0)
                            current.Age++;
                        else
                            current.Age--;

                        current.Age *= -1;
                        await s.SaveChangesAsync();
                        return false;
                    }
                }

                Assert.False(current == null, $"Expected to update '{id}' but could not load it in '{sp.ElapsedMilliseconds}' ms.");
            }

            if (current == null)
            {
                Assert.False(update, $"Expected to update '{id}' but could not load it.");

                current = new User();
                var age = Random.Shared.Next(1024);
                current.Age = age % 2 == 0 ? -1 : 1;
                await session.StoreAsync(current, id);
                return true;
            }

            Assert.True(current.Age != 0);

            if (current.Age > 0)
                current.Age++;
            else
                current.Age--;

            current.Age *= -1;

            return false;
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task ContinueSubscriptionAfterReshardingInACluster()
        {
            var cluster = await CreateRaftCluster(5, watcherCluster: true);
            using var store = Sharding.GetDocumentStore(new Options { Server = cluster.Leader, ReplicationFactor = 3, });

            await SubscriptionWithResharding(store);
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions, Skip = "Need to split this to drop orchestrator / drop shards")]
        public async Task ContinueSubscriptionAfterReshardingInAClusterWithFailover()
        {
            var cluster = await CreateRaftCluster(5, watcherCluster: true, shouldRunInMemory: false);
            using var store = Sharding.GetDocumentStore(new Options { Server = cluster.Leader, ReplicationFactor = 3, RunInMemory = false });

            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var t = Task.Run(async () => await ProcessSubscription(store, id, users, timoutSec: 30));
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
            {
                var fail = Task.Run(async () =>
                {
                    int position = -1;
                    (string DataDirectory, string Url, string NodeTag) result = default;
                    var recoveryOptions = new ServerCreationOptions
                    {
                        RunInMemory = false, DeletePrevious = false, RegisterForDisposal = true, CustomSettings = DefaultClusterSettings
                    };
                    recoveryOptions.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] =
                        cluster.Leader.Configuration.Cluster.ElectionTimeout.AsTimeSpan.TotalMilliseconds.ToString();


                    while (cts.IsCancellationRequested == false)
                    {
                        position = Random.Shared.Next(0, 5);
                        var node = cluster.Nodes[position];
                        if (node.ServerStore.IsLeader())
                            continue;
                        result = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
                        await Cluster.WaitForNodeToBeRehabAsync(store, result.NodeTag);
                        await Task.Delay(TimeSpan.FromSeconds(3));
                        cluster.Nodes[position] = await ReviveNodeAsync(result, recoveryOptions);
                        await Cluster.WaitForAllNodesToBeMembersAsync(store);
                    }
                });

                try
                {
                    var added1 = await CreateItems(store, 0, 2);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added2 = await CreateItems(store, 2, 4, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added3 = await CreateItems(store, 4, 6, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added4 = await CreateItems(store, 6, 7, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added5 = await CreateItems(store, 7, 8, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added6 = await CreateItems(store, 8, 10, update: true);
                }
                finally
                {
                    cts.Cancel();
                    await fail;
                    await t;
                }

                //  await PrintCollectionAndSubscriptionChangeVectors(store, cluster, id);

                await Indexes.WaitForIndexingInTheClusterAsync(store);
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

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id, cluster.Nodes);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task ContinueSubscriptionAfterReshardingInAClusterRF3WithOrchestratorFailover()
        {
            var cluster = await CreateRaftCluster(5, watcherCluster: true, shouldRunInMemory: false);
            // 5 node, 2 orch, 3 shard, rf 3 
            // drop orch

            var first3 = cluster.Nodes.Select(x => x.ServerStore.NodeTag).Take(3).ToList();
            var last2 = cluster.Nodes.Select(x => x.ServerStore.NodeTag).Skip(3).Take(2).ToList();
            Assert.All(first3, x => Assert.DoesNotContain(x, last2));
            var ops = new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Sharding ??= new ShardingConfiguration()
                    {
                        Shards = new Dictionary<int, DatabaseTopology>()
                        {
                            { 0, new DatabaseTopology() { ReplicationFactor = 3, Members = first3 } },
                            { 1, new DatabaseTopology() { ReplicationFactor = 3, Members = first3 } },
                            { 2, new DatabaseTopology() { ReplicationFactor = 3, Members = first3 } }
                        },
                        Orchestrator = new OrchestratorConfiguration { Topology = new OrchestratorTopology { ReplicationFactor = 2, Members = last2 } }
                    };
                },
                ReplicationFactor = 3,
                Server = cluster.Leader,
                RunInMemory = false
            };

            using var store = Sharding.GetDocumentStore(ops);
            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            int.TryParse(id, out var id2);

            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(id2, cluster.Nodes);

            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var t = Task.Run(async () => await ProcessSubscription(store, id, users, timoutSec: 60));

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
            {
                var fail = Task.Run(async () =>
                {
                    (string DataDirectory, string Url, string NodeTag) result = default;
                    var recoveryOptions = new ServerCreationOptions
                    {
                        RunInMemory = false, DeletePrevious = false, RegisterForDisposal = true, CustomSettings = DefaultClusterSettings
                    };
                    recoveryOptions.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] =
                        cluster.Leader.Configuration.Cluster.ElectionTimeout.AsTimeSpan.TotalMilliseconds.ToString();

                    while (cts.IsCancellationRequested == false)
                    {
                        var tag = last2[Random.Shared.Next(0, 2)];
                        var node = cluster.Nodes.First(x => x.ServerStore.NodeTag == tag);
                        if (node.ServerStore.IsLeader())
                            continue;

                        var position = cluster.Nodes.IndexOf(node);
                        result = await DisposeServerAndWaitForFinishOfDisposalAsync(node);

                        await Cluster.WaitForNodeToBeRehabAsync(store, result.NodeTag);
                        await Task.Delay(TimeSpan.FromSeconds(3));

                        Assert.Equal(cluster.Nodes[position].ServerStore.NodeTag, tag);
                        cluster.Nodes[position] = await ReviveNodeAsync(result, recoveryOptions);
                        await Cluster.WaitForAllNodesToBeMembersAsync(store);
                    }
                });

                try
                {
                    var added1 = await CreateItems(store, 0, 2);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added2 = await CreateItems(store, 2, 4, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added3 = await CreateItems(store, 4, 6, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added4 = await CreateItems(store, 6, 7, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added5 = await CreateItems(store, 7, 8, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added6 = await CreateItems(store, 8, 10, update: true);
                }
                finally
                {
                    cts.Cancel();
                    await fail;
                    await t;
                }
            }

            //  await PrintCollectionAndSubscriptionChangeVectors(store, cluster, id);

            await Indexes.WaitForIndexingInTheClusterAsync(store);
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

            await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id, cluster.Nodes);
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task ContinueSubscriptionAfterReshardingInAClusterRF1WithOrchestratorFailover()
        {
            var cluster = await CreateRaftCluster(5, watcherCluster: true, shouldRunInMemory: false);
            var first3 = cluster.Nodes.Select(x => x.ServerStore.NodeTag).Take(3).ToList();
            var last2 = cluster.Nodes.Select(x => x.ServerStore.NodeTag).Skip(3).Take(2).ToList();
            Assert.All(first3, x => Assert.DoesNotContain(x, last2));
            var ops = new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Sharding ??= new ShardingConfiguration()
                    {
                        Shards = new Dictionary<int, DatabaseTopology>()
                        {
                            { 0, new DatabaseTopology() { ReplicationFactor = 1, Members = new List<string>() { first3[0] } } },
                            { 1, new DatabaseTopology() { ReplicationFactor = 1, Members = new List<string>() { first3[1] } } },
                            { 2, new DatabaseTopology() { ReplicationFactor = 1, Members = new List<string>() { first3[2] } } }
                        },
                        Orchestrator = new OrchestratorConfiguration { Topology = new OrchestratorTopology { ReplicationFactor = 2, Members = last2 } }
                    };
                },
                ReplicationFactor = 1,
                Server = cluster.Leader,
                RunInMemory = false
            };

            using var store = Sharding.GetDocumentStore(ops);

            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            int.TryParse(id, out var id2);

            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(id2, cluster.Nodes);

            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var t = Task.Run(async () => await ProcessSubscription(store, id, users, timoutSec: 15));

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
            {
                var fail = Task.Run(async () =>
                {
                    (string DataDirectory, string Url, string NodeTag) result = default;
                    var recoveryOptions = new ServerCreationOptions
                    {
                        RunInMemory = false, DeletePrevious = false, RegisterForDisposal = true, CustomSettings = DefaultClusterSettings
                    };
                    recoveryOptions.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] =
                        cluster.Leader.Configuration.Cluster.ElectionTimeout.AsTimeSpan.TotalMilliseconds.ToString();


                    while (cts.IsCancellationRequested == false)
                    {
                        var tag = last2[Random.Shared.Next(0, 2)];
                        var node = cluster.Nodes.First(x => x.ServerStore.NodeTag == tag);
                        if (node.ServerStore.IsLeader())
                            continue;

                        var position = cluster.Nodes.IndexOf(node);
                        result = await DisposeServerAndWaitForFinishOfDisposalAsync(node);

                        await Cluster.WaitForNodeToBeRehabAsync(store, result.NodeTag);
                        await Task.Delay(TimeSpan.FromSeconds(3));

                        Assert.Equal(cluster.Nodes[position].ServerStore.NodeTag, tag);
                        cluster.Nodes[position] = await ReviveNodeAsync(result, recoveryOptions);
                        await Cluster.WaitForAllNodesToBeMembersAsync(store);
                    }
                });

                try
                {
                    var added1 = await CreateItems(store, 0, 2);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added2 = await CreateItems(store, 2, 4, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added3 = await CreateItems(store, 4, 6, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added4 = await CreateItems(store, 6, 7, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added5 = await CreateItems(store, 7, 8, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added6 = await CreateItems(store, 8, 10, update: true);
                }
                finally
                {
                    cts.Cancel();
                    await fail;
                    await t;
                }
            }

            //  await PrintCollectionAndSubscriptionChangeVectors(store, cluster, id);

            await Indexes.WaitForIndexingInTheClusterAsync(store);
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

            await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id, cluster.Nodes);
        }

        private async Task SubscriptionWithResharding(IDocumentStore store)
        {
            var id = await store.Subscriptions.CreateAsync<User>();
            var users = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var mre = new ManualResetEvent(false);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                             MaxDocsPerBatch = 5, TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
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

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 1, timeout: 60_000);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 1, timeout: 60_000);

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

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 2, timeout: 60_000);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 2, timeout: 60_000);

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

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 3, timeout: 60_000);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 3, timeout: 60_000);
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

                Assert.True(expected.Count == 0,
                    $"Missing {string.Join(Environment.NewLine, expected.Select(async e => $"{e} (shard: {await Sharding.GetShardNumberForAsync(store, e)})"))}");


                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }

        private int _current;
        private string NextId => $"users/{Interlocked.Increment(ref _current)}-A";

        private static ((string Id, int ShardNumber) Tuple1, (string Id, int ShardNumber) Tuple2) GetIdsOnDifferentShards(ShardingConfiguration conf,
            string collection = "users",
            int start = 1)
        {
            int shardNumber1;
            int shardNumber2;

            var c = start + 1;
            var firstId = $"{collection}/{start}-A";
            string secondId = string.Empty;
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                shardNumber1 = shardNumber2 = ShardHelper.GetShardNumberFor(conf, allocator, firstId);

                while (shardNumber2 == shardNumber1)
                {
                    secondId = $"{collection}/{c}-A";
                    shardNumber2 = ShardHelper.GetShardNumberFor(conf, allocator, secondId);
                    c++;
                }
            }

            Assert.NotEqual(shardNumber2, shardNumber1);
            Assert.NotEqual(secondId, firstId);
            return ((firstId, shardNumber1), (secondId, shardNumber2));
        }


        private static ((string Id, int ShardNumber, int BucketNumber) Tuple1, (string Id, int ShardNumber, int BucketNumber) Tuple2) GetIdsOnDifferentShardsAndBuckets(
            ShardingConfiguration conf, string collection = "users",
            int start = 1)
        {
            int shardNumber1;
            int bucketNumber1;
            int shardNumber2;
            int bucketNumber2;

            var c = start + 1;
            var firstId = $"{collection}/{start}-A";
            string secondId = string.Empty;
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                (shardNumber1, bucketNumber1) = (shardNumber2, bucketNumber2) = ShardHelper.GetShardNumberAndBucketFor(conf, allocator, firstId);

                while (shardNumber2 == shardNumber1 || bucketNumber2 == bucketNumber1)
                {
                    secondId = $"{collection}/{c}-A";
                    (shardNumber2, bucketNumber2) = ShardHelper.GetShardNumberAndBucketFor(conf, allocator, secondId);
                    c++;
                }
            }

            Assert.NotEqual(shardNumber2, shardNumber1);
            Assert.NotEqual(bucketNumber2, bucketNumber1);
            Assert.NotEqual(secondId, firstId);
            return ((firstId, shardNumber1, bucketNumber1), (secondId, shardNumber2, bucketNumber2));
        }

        private static ((string Id, int ShardNumber, int BucketNumber) Tuple1, (string Id, int ShardNumber, int BucketNumber) Tuple2)
            GetIdsOnSameShardAndDifferentBuckets(ShardingConfiguration conf, string collection = "users",
                int start = 1)
        {
            int shardNumber1;
            int bucketNumber1;
            int shardNumber2;
            int bucketNumber2;

            var c = start + 1;
            var firstId = $"{collection}/{start}-A";
            string secondId = string.Empty;
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                (shardNumber1, bucketNumber1) = (shardNumber2, bucketNumber2) = ShardHelper.GetShardNumberAndBucketFor(conf, allocator, firstId);
                var f = true;
                while (f)
                {
                    secondId = $"{collection}/{c}-A";
                    (shardNumber2, bucketNumber2) = ShardHelper.GetShardNumberAndBucketFor(conf, allocator, secondId);

                    if (shardNumber2 == shardNumber1)
                    {
                        if (bucketNumber2 != bucketNumber1)
                        {
                            f = false;
                        }
                    }

                    c++;
                }
            }

            Assert.Equal(shardNumber2, shardNumber1);
            Assert.NotEqual(bucketNumber2, bucketNumber1);
            Assert.NotEqual(secondId, firstId);

            return ((firstId, shardNumber1, bucketNumber1), (secondId, shardNumber2, bucketNumber2));
        }


        private async Task PrintCollectionAndSubscriptionChangeVectors(DocumentStore store, (List<RavenServer> Nodes, RavenServer Leader) cluster, string id)
        {
            var databaseContext = Sharding.GetOrchestratorInCluster(store.Database, cluster.Nodes);

            var shardExecutor = databaseContext.ShardExecutor;
            var ctx = new DefaultHttpContext();

            var changeVectorsCollection =
                (await shardExecutor.ExecuteParallelForAllAsync(
                    new ShardedLastChangeVectorForCollectionOperation(ctx.Request, "Users", databaseContext.DatabaseName))).LastChangeVectors;
            Console.WriteLine("Users Collection changeVectors:");
            foreach (var cv in changeVectorsCollection.OrderBy(x => x.Key))
            {
                Console.WriteLine($"{cv.Key}: {cv.Value}");
            }


            var state = store.Subscriptions.GetSubscriptionState(id);

            Console.WriteLine("ChangeVectorForNextBatchStartingPointPerShard:");
            foreach (var cv in state.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.OrderBy(x => x.Key))
            {
                Console.WriteLine($"{cv.Key}: {cv.Value}");
            }

            Console.WriteLine("ChangeVectorForNextBatchStartingPointForOrchestrator:");
            Console.WriteLine(state.ShardingState.ChangeVectorForNextBatchStartingPointForOrchestrator);
        }
    }
}
