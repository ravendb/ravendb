using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Server;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Subscriptions
{
    public class ShardedSubscriptionSlowTests : RavenTestBase
    {
        public ShardedSubscriptionSlowTests(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(30);

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task AcknowledgeSubscriptionBatchWhenDBisBeingDeletedShouldThrow()
        {
            using var store = Sharding.GetDocumentStore();
            var id = await store.Subscriptions.CreateAsync<User>();
            var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
            Assert.Equal(1, subscriptions.Count);
            var realId = subscriptions.First().SubscriptionId;
            using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "EGR",
                        Age = 39
                    }, Guid.NewGuid().ToString());
                    session.SaveChanges();
                }
                var t = Task.Run(async () => await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true)));

                Exception ex = null;
                try
                {
                    await subscription.Run(x => { }).WaitAsync(_reasonableWaitTime);
                }
                catch (Exception e)
                {
                    ex = e;
                }
                finally
                {
                    Assert.NotNull(ex);
                    Assert.True(ex is DatabaseDoesNotExistException || ex is SubscriptionDoesNotExistException);

                    Assert.Contains(
                        ex is SubscriptionDoesNotExistException
                            ? $"Subscription With Id '{realId}' cannot be opened, because it does not exist."
                            : $"Database '{store.Database}' does not exist.", ex.Message);
                }

                await t;
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task CanUpdateSubscriptionToStartFromBeginningOfTime()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var count = 10;
                store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(1, subscriptions.Count);

                var state = subscriptions.First();
                Assert.Equal("from 'Users' as doc", state.Query);

                const string newQuery = "from Users where Age > 18";

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(state.SubscriptionName)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(1000)
                }))
                {
                    Exception onSubscriptionConnectionRetryException = null;
                    subscription.OnSubscriptionConnectionRetry += x =>
                    {
                        onSubscriptionConnectionRetryException = x;
                    };

                    using var first = new CountdownEvent(count);
                    using var second = new CountdownEvent(count / 2);

                    var t = subscription.Run(x =>
                    {
                        if (first.IsSet)
                            second.Signal(x.NumberOfItemsInBatch);
                        else
                            first.Signal(x.NumberOfItemsInBatch);
                    });

                    for (int i = 0; i < count; i++)
                    {
                        var age = i < (count / 2) ? 18 : 19;
                        using (var session = store.OpenSession())
                        {
                            session.Store(new User
                            {
                                Name = $"EGR_{i}",
                                Age = age
                            }, Guid.NewGuid().ToString());
                            session.SaveChanges();
                        }
                    }

                    Assert.True(first.Wait(_reasonableWaitTime));
                    await store.Subscriptions.UpdateAsync(new SubscriptionUpdateOptions
                    {
                        Name = state.SubscriptionName,
                        Query = newQuery,
                        ChangeVector = $"{Constants.Documents.SubscriptionChangeVectorSpecialStates.BeginningOfTime}"
                    });

                    var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                    var newState = newSubscriptions.First();
                    Assert.Equal(1, newSubscriptions.Count);
                    Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                    Assert.Equal(newQuery, newState.Query);
                    Assert.Equal(state.SubscriptionId, newState.SubscriptionId);

                    await CheckSubscriptionNewQuery(store, state, newQuery);
                    Assert.True(second.Wait(_reasonableWaitTime));

                    AssertOnSubscriptionConnectionRetryEventException(onSubscriptionConnectionRetryException, state);
                }
            }
        }

        private static void AssertOnSubscriptionConnectionRetryEventException(Exception OnSubscriptionConnectionRetryException, SubscriptionState state)
        {
            switch (OnSubscriptionConnectionRetryException)
            {
                case SubscriptionClosedException sce:
                    Assert.True(sce.CanReconnect);
                    Assert.Equal(
                        $"Subscription With Id '{state.SubscriptionName}' was closed.  Raven.Client.Exceptions.Documents.Subscriptions.SubscriptionClosedException: The subscription {state.SubscriptionName} query has been modified, connection must be restarted",
                        OnSubscriptionConnectionRetryException.Message);
                    break;
                case SubscriptionChangeVectorUpdateConcurrencyException:
                    // sometimes we may hit cv concurrency exception because of the update
                    Assert.StartsWith(
                        $"Can't acknowledge subscription with name '{state.SubscriptionName}' due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value. Stored value: , received value: A:11",
                        OnSubscriptionConnectionRetryException.Message);
                    break;
            }
        }

        private async Task CheckSubscriptionNewQuery(IDocumentStore store, SubscriptionState state, string newQuery)
        {
            var shards = Sharding.GetShardsDocumentDatabaseInstancesFor(store);
            await foreach (var db in shards)
            {
                var query = WaitForValue(() =>
                {
                    using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var connectionState = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, state.SubscriptionName);
                        return connectionState?.GetConnections().FirstOrDefault()?.SubscriptionState.Query;
                    }
                }, newQuery);

                Assert.Equal(newQuery, query);
            }
        }

        private async Task CheckSubscriptionNewCVsAsync(IDocumentStore store, SubscriptionState state, Dictionary<string, string> cvs)
        {
            var shards = Sharding.GetShardsDocumentDatabaseInstancesFor(store);
            await foreach (var db in shards)
            {
                var newCv = cvs[db.Name];
                var cv = WaitForValue(() =>
                {
                    using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var connectionState = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, state.SubscriptionName);
                        if (connectionState != null)
                        {
                            var connection = connectionState.GetConnections().FirstOrDefault();
                            if (connection != null)
                            {
                                var subsState = connection.SubscriptionState;
                                if (subsState.ShardingState != null)
                                {
                                    if (subsState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(db.Name, out string cv))
                                    {
                                        return cv;
                                    }

                                    return null;
                                }

                                return null;
                            }

                            return null;
                        }

                        return null;
                    }
                }, newCv, interval: 333);

                Assert.Equal(newCv, cv);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanUpdateSubscriptionToStartFromLastDocument(bool updateToSameCv)
        {
            var ops = updateToSameCv
                ? new Options
                {
                    ModifyDatabaseRecord = record =>
                    {
                        record.Sharding ??= new ShardingConfiguration()
                        {
                            Shards = new Dictionary<int, DatabaseTopology>() { { 0, new DatabaseTopology() }, { 1, new DatabaseTopology() } }
                        };
                    }
                }
                : null;
            using (var store = Sharding.GetDocumentStore(ops))
            {
                var count = 10;
                store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(1, subscriptions.Count);

                var state = subscriptions.First();
                Assert.Equal("from 'Users' as doc", state.Query);

                using var docs = new CountdownEvent(count / 2);
                TimeSpan fromMilliseconds = TimeSpan.FromMilliseconds(16);
                using var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(state.SubscriptionName)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(16),
                    MaxDocsPerBatch = 1
                });

                var t = subscription.Run(async x =>
                {
                    foreach (var user in x.Items)
                    {
                        await Task.Delay(fromMilliseconds);
                    }
                });

                var ackDocs = new List<string>();
                var mre = new AsyncManualResetEvent();
                var mre2 = new AsyncManualResetEvent();
                var f = true;
                subscription.AfterAcknowledgment += async batch =>
                {
                    foreach (var user in batch.Items)
                    {
                        // there should be only 1 doc acked
                        ackDocs.Add(user.Id);

                        if (user.Result.Age > 18)
                        {
                            docs.Signal(1);
                        }
                    }

                    if (f)
                    {
                        mre.Set();
                        Assert.True(await mre2.WaitAsync(_reasonableWaitTime));
                    }
                };

                var addedDocs = new List<User>();

                if (updateToSameCv)
                {
                    using (var session = store.OpenSession())
                    {
                        var id11 = Guid.NewGuid().ToString() + "$users2";
                        var user11 = new User { Name = $"EGR_{123}", Age = 18 - 5 };
                        session.Store(user11, id11);

                        user11.Id = id11;
                        addedDocs.Add(user11);
                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        string id = null;

                        if (updateToSameCv)
                        {
                            id = Guid.NewGuid().ToString() + "$users1";
                        }
                        else
                        {
                            id = Guid.NewGuid().ToString();
                        }

                        var user = new User { Name = $"EGR_{i}", Age = 18 - i };
                        session.Store(user, id);

                        user.Id = id;
                        addedDocs.Add(user);
                    }

                    session.SaveChanges();
                }

                Assert.True(await mre.WaitAsync(_reasonableWaitTime));

                var f1 = addedDocs.First();
                var f2 = ackDocs.First();

                // there should be only 1 doc acked
                Assert.Equal(1, ackDocs.Count);
                Assert.Contains(f2, addedDocs.Select(x => x.Id));
                await store.Subscriptions.UpdateAsync(new SubscriptionUpdateOptions
                {
                    Name = state.SubscriptionName,
                    ChangeVector = $"{Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument}"
                });
                f = false;

                List<SubscriptionState> newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(state.SubscriptionId, newState.SubscriptionId);

                DatabasesLandlord.DatabaseSearchResult result = Server.ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(store.Database);
                Assert.Equal(DatabasesLandlord.DatabaseSearchResult.Status.Sharded, result.DatabaseStatus);
                Assert.NotNull(result.DatabaseContext);
                var shardExecutor = result.DatabaseContext.ShardExecutor;
                var ctx = new DefaultHttpContext();

                var changeVectorsCollection =
                    (await shardExecutor.ExecuteParallelForAllAsync(
                        new ShardedLastChangeVectorForCollectionOperation(ctx.Request, "Users", result.DatabaseContext.DatabaseName))).LastChangeVectors;

                if (updateToSameCv)
                {
                    Assert.Equal(2, changeVectorsCollection.Count);
                }
                else
                {
                    Assert.Equal(3, changeVectorsCollection.Count);
                }

                mre2.Set();

                await CheckSubscriptionNewCVsAsync(store, state, changeVectorsCollection);

                // connect and assert no docs are sent 
                // after setting subs id to LastDocument, we add Users with Age>18
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < count / 2; i++)
                    {
                        var id = Guid.NewGuid().ToString();
                        var user = new User { Name = $"EGR_{i}", Age = 19 + i };
                        session.Store(user, id);

                        user.Id = id;
                        addedDocs.Add(user);
                    }

                    session.SaveChanges();
                }

                Assert.True(docs.Wait(_reasonableWaitTime));

                var added18OrLowerIds = addedDocs.Where(x => x.Age <= 18);
                var added18PlusIds = addedDocs.Where(x => x.Age > 18);
                Assert.True(ackDocs.Count < 10);
                Assert.All(added18PlusIds, u =>
                {
                    Assert.Contains(u.Id, ackDocs);
                });

                Assert.True(added18OrLowerIds.Any(u => ackDocs.Contains(u.Id) == false), "We didn't skip any docs");
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task CanUpdateSubscriptionToStartFromDoNotChange()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var count = 10;
                store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(1, subscriptions.Count);

                var state = subscriptions.First();
                Assert.Equal("from 'Users' as doc", state.Query);

                using var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(state.SubscriptionName)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(16)
                });
                Exception onSubscriptionConnectionRetryException = null;
                subscription.OnSubscriptionConnectionRetry += x =>
                {
                    onSubscriptionConnectionRetryException = x;
                };
                using var docs = new CountdownEvent(count);

                var t = subscription.Run(x => docs.Signal(x.NumberOfItemsInBatch));

                for (int i = 0; i < count / 2; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = $"EGR_{i}",
                            Age = 18
                        }, Guid.NewGuid().ToString());
                        session.SaveChanges();
                    }
                }

                Assert.Equal(count / 2, WaitForValue(() => docs.CurrentCount, count / 2));
                const string newQuery = "from Users where Age > 18";

                store.Subscriptions.Update(new SubscriptionUpdateOptions
                {
                    Name = state.SubscriptionName,
                    Query = newQuery,
                    ChangeVector = $"{Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange}"
                });

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(newQuery, newState.Query);
                Assert.Equal(state.SubscriptionId, newState.SubscriptionId);

                await CheckSubscriptionNewQuery(store, state, newQuery);

                for (int i = 0; i < count / 2; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = $"EGR_{i}",
                            Age = 19
                        }, Guid.NewGuid().ToString());
                        session.SaveChanges();
                    }
                }

                Assert.True(docs.Wait(_reasonableWaitTime));
                AssertOnSubscriptionConnectionRetryEventException(onSubscriptionConnectionRetryException, state);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        public void ThrowOnUpdatingChangeVectorByAdmin()
        {
            //RavenDB-18223
            using (var store = Sharding.GetDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<User>()
                {
                    ChangeVector = Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument.ToString()
                });

                Assert.Throws<RavenException>(() => store.Subscriptions.Update(new SubscriptionUpdateOptions() { Name = id, ChangeVector = "A:322-AaAaAaAaAaAa" }));
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        public void ThrowOnSettingChangeVectorByAdmin()
        {
            //RavenDB-18223
            using (var store = Sharding.GetDocumentStore())
            {
                Assert.Throws<RavenException>(() => store.Subscriptions.Create(new SubscriptionCreationOptions<User>() { ChangeVector = "A:322-AaAaAaAaAaAa" }));
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding, Skip = "RavenDB-18223: Cannot set CV by admin in sharded subscription")]
        public async Task RunningSubscriptionShouldJumpToNextChangeVectorIfItWasChangedByAdmin()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionId)
                {
                    MaxDocsPerBatch = 1,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var users = new BlockingCollection<User>();
                    string cvFirst = null;
                    string cvBigger = null;
                    var ackFirstCV = new AsyncManualResetEvent();
                    var ackUserPast = new AsyncManualResetEvent();
                    var items = new ConcurrentBag<User>();
                    subscription.AfterAcknowledgment += batch =>
                    {
                        var changeVector = batch.Items.Last().ChangeVector.ToChangeVector();
                        var savedCV = cvFirst.ToChangeVector();
                        if (changeVector[0].Etag >= savedCV[0].Etag)
                        {
                            ackFirstCV.Set();
                        }
                        foreach (var item in batch.Items)
                        {
                            items.Add(item.Result);
                            if (item.Result.Age >= 40)
                                ackUserPast.Set();
                        }
                        return Task.CompletedTask;
                    };

                    using (var session = store.OpenSession())
                    {
                        var newUser = new User
                        {
                            Name = "James",
                            Age = 20
                        };
                        session.Store(newUser, "users/1");
                        session.SaveChanges();
                        var metadata = session.Advanced.GetMetadataFor(newUser);
                        cvFirst = (string)metadata[Raven.Client.Constants.Documents.Metadata.ChangeVector];
                    }
                    var t = subscription.Run(x =>
                    {
                        foreach (var i in x.Items)
                        {
                            users.Add(i.Result);
                        }
                    });

                    var firstItemchangeVector = cvFirst.ToChangeVector();
                    var cvNew = new List<ChangeVectorEntry>();

                    await foreach (var db in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
                    {
                        cvNew.Add(new ChangeVectorEntry()
                        {
                            DbId = db.DbBase64Id,
                            NodeTag = firstItemchangeVector[0].NodeTag,
                            Etag = firstItemchangeVector[0].Etag + 10
                        });
                    }

                    cvBigger = cvNew.ToArray().SerializeVector();
                    Assert.True(await ackFirstCV.WaitAsync(_reasonableWaitTime));

                    DatabasesLandlord.DatabaseSearchResult result = Server.ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(store.Database);
                    Assert.Equal(DatabasesLandlord.DatabaseSearchResult.Status.Sharded, result.DatabaseStatus);
                    Assert.NotNull(result.DatabaseContext);

                    SubscriptionState subscriptionState;
                    using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        subscriptionState = result.DatabaseContext.SubscriptionsStorage.GetSubscriptionByName(context, subscriptionId);
                    }

                    await foreach (var db in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
                    {
                        var connectionState = db.SubscriptionStorage.PutSubscription(new SubscriptionCreationOptions()
                        {
                            ChangeVector = cvBigger,
                            Name = subscriptionState.SubscriptionName,
                            Query = subscriptionState.Query
                        }, Guid.NewGuid().ToString(), subscriptionState.SubscriptionId);

                        break;
                    }

                    using (var session = store.OpenSession())
                    {
                        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Normal, "some why they all go to 1 shard");
                        //// here we have to make sure that at least 10 docs saved to each shard
                        // TODO: egor some why they all go to 1 shard
                        //using (var session = store.OpenSession())
                        //{
                        //    session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
                        //    // here we have to make sure that at least 10 docs saved to each shard
                        //    var servers = await ShardedClusterTestBase.GetShardsDocumentDatabaseInstancesFor(store, new List<RavenServer> { Server });
                        //    var i = 0;
                        //    while (AllShardHaveDocs(servers, count: 15) == false)
                        //    {
                        //        session.Store(new User { Name = "Adam", Age = 21 + i }, "users/");
                        //        session.SaveChanges();
                        //        i++;
                        //    }
                        //}

                        for (var i = 0; i < 100; i++)
                        {
                            session.Store(new User
                            {
                                Name = "Adam",
                                Age = 21 + i
                            }, "users/");
                        }
                        session.SaveChanges();
                    }

                    Assert.True(await ackUserPast.WaitAsync(_reasonableWaitTime));

                    foreach (var item in items)
                    {
                        if (item.Age > 20 && item.Age < 30)
                            Assert.True(false, "Got age " + item.Age);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanUseSubscriptionWithDocumentIncludes(bool diff)
        {
            DoNotReuseServer();
            Server.ServerStore.Sharding.BlockPrefixedSharding = false;

            var ops = diff
                ? new Options
                {
                    ModifyDatabaseRecord = record =>
                    {
                        record.Sharding ??= new ShardingConfiguration();
                        record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                        {
                            new PrefixedShardingSetting { Prefix = "people/", Shards = new List<int> { 0 } },
                            new PrefixedShardingSetting { Prefix = "dogs/", Shards = new List<int> { 1, 2 } }
                        };
                    }
                }
                : null;
            using (var store = Sharding.GetDocumentStore(ops))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Arava"
                    }, "people/1");
                    session.Store(new Dog
                    {
                        Name = "Oscar",
                        Owner = "people/1"
                    });
                    session.SaveChanges();
                }
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions
                {
                    Query = @"from Dogs include Owner"
                });
                using (var sub = store.Subscriptions.GetSubscriptionWorker<Dog>(id))
                {
                    var mre = new AsyncManualResetEvent();
                    var r = sub.Run(batch =>
                    {
                        Assert.NotEmpty(batch.Items);
                        using (var s = batch.OpenSession())
                        {
                            foreach (var item in batch.Items)
                            {
                                s.Load<Person>(item.Result.Owner);
                                var dog = s.Load<Dog>(item.Id);
                                Assert.Same(dog, item.Result);
                            }
                            Assert.Equal(0, s.Advanced.NumberOfRequests);
                        }
                        mre.Set();
                    });
                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(60)));
                    await sub.DisposeAsync();
                    await r;// no error
                }

            }
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        public void CanUseSubscriptionWithCountersIncludesWhenAllExist()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                string name = store.Subscriptions
                    .Create(new SubscriptionCreationOptions<Company>() { Includes = builder => builder.IncludeAllCounters() });


                using (var session = store.OpenSession())
                {
                    var company = new Company { Id = "companies/1", Name = "HR" };
                    session.Store(company);
                    session.CountersFor(company).Increment("Likes", 322);
                    session.CountersFor(company).Increment("Dislikes", 228);
                    session.SaveChanges();
                }

                var mre = new ManualResetEventSlim();
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(name);

                long? likes = null;
                long? dislikes = null;
                int? numberOfRequests = null;

                var t = worker.Run(batch =>
                {
                    using (var session = batch.OpenSession())
                    {
                        var company = session.Load<Company>("companies/1");
                        var counters = session.CountersFor(company);
                        likes = counters.Get("Likes");
                        dislikes = counters.Get("Dislikes");
                        numberOfRequests = session.Advanced.NumberOfRequests;
                    }

                    mre.Set();
                });

                var result = WaitForValue(() => mre.Wait(TimeSpan.FromSeconds(500)), true);
                if (result == false && t.IsFaulted)
                    Assert.True(result, $"t.IsFaulted: {t.Exception}, {t.Exception?.InnerException}");

                Assert.True(result);

                Assert.Equal(322, likes);
                Assert.Equal(228, dislikes);
                Assert.Equal(0, numberOfRequests);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        public void CanUseSubscriptionWithCountersIncludesWhenWithNonExisting()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                string name = store.Subscriptions
                        .Create(new SubscriptionCreationOptions<Company>()
                        {
                            Includes = builder => builder.IncludeCounters(new[] { "Likes", "Subscribes" })
                        });

                using (var session = store.OpenSession())
                {
                    var company = new Company { Id = "companies/1", Name = "HR" };
                    session.Store(company);
                    session.CountersFor(company).Increment("Likes", 322);
                    session.CountersFor(company).Increment("Dislikes", 228);
                    session.SaveChanges();
                }

                var mre = new ManualResetEventSlim();
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(name);

                long? likes = null;
                long? subscribes = 0;
                int? numberOfRequests = null;
                var t = worker.Run(batch =>
                {
                    using (var session = batch.OpenSession())
                    {
                        var company = session.Load<Company>("companies/1");
                        likes = session.CountersFor(company).Get("Likes");
                        subscribes = session.CountersFor(company).Get("Subscribes");
                        numberOfRequests = session.Advanced.NumberOfRequests;
                    }

                    mre.Set();
                });

                var result = WaitForValue(() => mre.Wait(TimeSpan.FromSeconds(500)), true);
                if (result == false && t.IsFaulted)
                    Assert.True(result, $"t.IsFaulted: {t.Exception}, {t.Exception?.InnerException}");

                Assert.True(result);

                Assert.Equal(322, likes);
                Assert.Equal(null, subscribes);
                Assert.Equal(0, numberOfRequests);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        [InlineData(true)]
        [InlineData(false)]
        public void CanUseSubscriptionWithCountersIncludesWhenWithAllNonExist(bool single)
        {
            using (var store = Sharding.GetDocumentStore())
            {
                string name = null;
                if (single == false)
                {
                    name = store.Subscriptions
                        .Create(new SubscriptionCreationOptions<Company>() { Includes = builder => builder.IncludeCounters(new[] { "Shares", "Subscribes" }) });
                }
                else
                {
                    name = store.Subscriptions
                        .Create(new SubscriptionCreationOptions<Company>() { Includes = builder => builder.IncludeCounter("Shares") });
                }

                using (var session = store.OpenSession())
                {
                    var company = new Company { Id = "companies/1", Name = "HR" };
                    session.Store(company);
                    session.CountersFor(company).Increment("Likes", 322);
                    session.CountersFor(company).Increment("Dislikes", 228);
                    session.SaveChanges();
                }

                var mre = new ManualResetEventSlim();
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(name);

                long? shares = 0;
                long? subscribes = 0;
                int? numberOfRequests = null;
                var t = worker.Run(batch =>
                {
                    using (var session = batch.OpenSession())
                    {
                        var company = session.Load<Company>("companies/1");
                        shares = session.CountersFor(company).Get("Shares");
                        if (single == false)
                        {
                            subscribes = session.CountersFor(company).Get("Subscribes");
                        }
                        numberOfRequests = session.Advanced.NumberOfRequests;
                    }

                    mre.Set();
                });

                var result = WaitForValue(() => mre.Wait(TimeSpan.FromSeconds(500)), true);
                if (result == false && t.IsFaulted)
                    Assert.True(result, $"t.IsFaulted: {t.Exception}, {t.Exception?.InnerException}");

                Assert.True(result);

                Assert.Equal(null, shares);
                if (single == false)
                {
                    Assert.Equal(null, subscribes);
                }
                else
                {
                    Assert.Equal(0, subscribes);
                }

                Assert.Equal(0, numberOfRequests);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        [InlineData(true)]
        [InlineData(false)]
        public void CanCreateSubscriptionWithIncludeTimeSeries_All_LastRange(bool byTime)
        {
            var now = DateTime.UtcNow.EnsureMilliseconds();

            using (var store = Sharding.GetDocumentStore())
            {
                string name;
                if (byTime)
                {
                    name = store.Subscriptions
                        .Create(new SubscriptionCreationOptions<Company>()
                        {
                            Includes = builder => builder
                                .IncludeAllTimeSeries(TimeSeriesRangeType.Last, TimeValue.FromDays(7))
                        });
                }
                else
                {
                    name = store.Subscriptions
                        .Create(new SubscriptionCreationOptions<Company>()
                        {
                            Includes = builder => builder
                                .IncludeAllTimeSeries(TimeSeriesRangeType.Last, count: 32)
                        });
                }

                var mre = new ManualResetEventSlim();
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(name);
                var t = worker.Run(batch =>
                {
                    using (var session = batch.OpenSession())
                    {
                        Assert.Equal(0, session.Advanced.NumberOfRequests);

                        var company = session.Load<Company>("companies/1");
                        Assert.Equal(0, session.Advanced.NumberOfRequests);

                        var timeSeries = session.TimeSeriesFor(company, "StockPrice");
                        var timeSeriesEntries = timeSeries.Get(from: now.AddDays(-7));

                        Assert.Equal(1, timeSeriesEntries.Length);
                        Assert.Equal(now.AddDays(-7), timeSeriesEntries[0].Timestamp);
                        Assert.Equal(10, timeSeriesEntries[0].Value);

                        Assert.Equal(0, session.Advanced.NumberOfRequests);

                        timeSeries = session.TimeSeriesFor(company, "StockPrice2");
                        timeSeriesEntries = timeSeries.Get(from: now.AddDays(-5));

                        Assert.Equal(1, timeSeriesEntries.Length);
                        Assert.Equal(now.AddDays(-5), timeSeriesEntries[0].Timestamp);
                        Assert.Equal(100, timeSeriesEntries[0].Value);

                        Assert.Equal(0, session.Advanced.NumberOfRequests);
                    }

                    mre.Set();
                });

                using (var session = store.OpenSession())
                {
                    var company = new Company { Id = "companies/1", Name = "HR" };
                    session.Store(company);

                    session.TimeSeriesFor(company, "StockPrice").Append(now.AddDays(-7), 10);
                    session.TimeSeriesFor(company, "StockPrice2").Append(now.AddDays(-5), 100);

                    session.SaveChanges();
                }

                var result = WaitForValue(() => mre.Wait(TimeSpan.FromSeconds(500)), true);
                if (result == false && t.IsFaulted)
                    Assert.True(result, $"t.IsFaulted: {t.Exception}, {t.Exception?.InnerException}");

                Assert.True(result);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding, Skip = "RavenDB-19889")]
        [InlineData(true)]
        [InlineData(false)]
        public void SubscriptionWithIncludeAllCountersOfDocumentAndOfRelatedDocument(bool diff)
        {
            DoNotReuseServer();
            Server.ServerStore.Sharding.BlockPrefixedSharding = false;

            var ops = diff
                ? new Options
                {
                    ModifyDatabaseRecord = record =>
                    {
                        record.Sharding ??= new ShardingConfiguration();
                        record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                        {
                            new PrefixedShardingSetting { Prefix = "people/", Shards = new List<int> { 0 } },
                            new PrefixedShardingSetting { Prefix = "dogs/", Shards = new List<int> { 1, 2 } }
                        };
                    }
                }
                : null;
            using (var store = Sharding.GetDocumentStore(ops))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "Arava" }, "people/1");
                    session.Store(new Dog { Name = "Oscar", Owner = "people/1" }, "dogs/1");
                    // session.Store(new Person { Name = "Arava2" }, "people/2");
                    // session.Store(new Dog { Name = "Oscar2", Owner = "people/2" }, "dogs/2");

                    session.CountersFor("people/1").Increment("Dogs");
                    //   session.CountersFor("people/2").Increment("Dogs");
                    session.CountersFor("dogs/1").Increment("Barks", 15);
                    //  session.CountersFor("dogs/2").Increment("Barks", 32);

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCreationOptions { Query = @"from Dogs as dog include counters(dog),counters(dog.Owner)" });

                /* Example from queries: 
                   Assert.Equal("from 'Orders' as x " +
                "include counters(x),counters(x.Employee)"*/

                var mre = new ManualResetEventSlim();
                var worker = store.Subscriptions.GetSubscriptionWorker<Dog>(id);

                int? numberOfRequests = null;
                Dictionary<string, long?> dic1 = null;
                Dictionary<string, long?> dic2 = null;
                var t = worker.Run(batch =>
                {
                    using (var session = batch.OpenSession())
                    {
                        //      var person = session.Load<Person>("people/1");
                        dic1 = session.CountersFor("people/1").GetAll();
                        dic2 = session.CountersFor("dogs/1").GetAll();
                        numberOfRequests = session.Advanced.NumberOfRequests;
                    }

                    mre.Set();
                });

                var result = WaitForValue(() => mre.Wait(TimeSpan.FromSeconds(500)), true);
                if (result == false && t.IsFaulted)
                    Assert.True(result, $"t.IsFaulted: {t.Exception}, {t.Exception?.InnerException}");

                Assert.True(result);
                Assert.Equal(1, dic1.Count);
                Assert.Equal(1, dic1["Dogs"]);
                Assert.Equal(1, dic2.Count);
                Assert.Equal(15, dic2["Barks"]);

                Assert.Equal(0, numberOfRequests);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding, Skip = "RavenDB-18568")]
        public async Task ConcurrentSubscriptions()
        {
            using (var store = Sharding.GetDocumentStore())
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
                    await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding, Skip = "RavenDB-18568")]
        public async Task CanGetSubscriptionsResendItems()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    MaxDocsPerBatch = 1
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
                    var amre = new AsyncManualResetEvent();
                    var amre2 = new AsyncManualResetEvent();
                    var t = subscription.Run(async x =>
                    {
                        foreach (var item in x.Items)
                        {
                            if (item.Id == "user/6")
                            {
                                amre2.Set();
                                await amre.WaitAsync(_reasonableWaitTime);
                            }

                            con1Docs.Add(item.Id);
                        }
                    });

                    await amre2.WaitAsync(_reasonableWaitTime);
                    await AssertWaitForTrueAsync(async () =>
                    {
                        var batches = await store.Maintenance.SendAsync(new GetSubscriptionBatchesStateOperation(subscription.SubscriptionName));
                        return batches.Results.ToList().Exists(b => b.Id == "user/6");
                    });

                    amre.Set();

                    await AssertWaitForTrueAsync(() => Task.FromResult(con1Docs.Count == 6), 6000);
                    await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
                }
            }
        }


        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        public async Task AbortWhenNoDocsLeft2()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                for (int j = 0; j < 100; j++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            await session.StoreAsync(new User());
                        }

                        await session.SaveChangesAsync();
                    }
                }

                var sn = await store.Subscriptions.CreateAsync<User>();
                var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sn)
                {
                    CloseWhenNoDocsLeft = true,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    MaxDocsPerBatch = 16
                });

                var items = 0;
                var st = worker.Run(x => { Interlocked.Add(ref items, x.NumberOfItemsInBatch); });
                await Assert.ThrowsAsync<SubscriptionClosedException>(() => st.WaitWithoutExceptionAsync(_reasonableWaitTime));
                Assert.Equal(1000, items);
                await worker.DisposeAsync();

                var worker2 = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sn)
                {
                    CloseWhenNoDocsLeft = true,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    MaxDocsPerBatch = 16
                });

                var gotBatch = false;
                var st2 = worker2.Run(x =>
                {
                    if (x.NumberOfItemsInBatch > 0)
                        gotBatch = true;
                });
                await Assert.ThrowsAsync<SubscriptionClosedException>(() => st2.WaitWithoutExceptionAsync(_reasonableWaitTime));
                Assert.False(gotBatch);
            }
        }


        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding | RavenTestCategory.BackupExportImport)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanExportImportAndRunSubscriptionsShardedAndNonSharded(bool fromSharded)
        {
            var file = Path.GetTempFileName();
            try
            {
                DocumentStore store1;
                DocumentStore store2;

                if (fromSharded)
                {
                    store1 = Sharding.GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_1" });
                    store2 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" });
                }
                else
                {
                    store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_1" });
                    store2 = Sharding.GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" });
                }

                using (store1)
                using (store2)
                {
                    await store1.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User> { Name = "sub1" });
                    await store1.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User> { Name = "sub2" });
                    await store1.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

                    var states = store1.Subscriptions.GetSubscriptions(0, 10);

                    Assert.Equal(3, states.Count);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    states = store2.Subscriptions.GetSubscriptions(0, 10, store2.Database);

                    Assert.Equal(3, states.Count);
                    Assert.True(states.Any(x => x.SubscriptionName.Equals("sub1")));
                    Assert.True(states.Any(x => x.SubscriptionName.Equals("sub2")));

                    using (var session = store2.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                        await session.SaveChangesAsync();
                    }

                    var mre = new AsyncManualResetEvent();
                    using (var worker = store2.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("sub1")
                    {
                        MaxDocsPerBatch = 5,
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1)
                    }))
                    {
                        var t = worker.Run(_ =>
                        {
                            mre.Set();
                        });

                        Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding | RavenTestCategory.BackupExportImport)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanBackupRestoreAndRunSubscriptionsShardedAndNonSharded(bool fromSharded)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            DocumentStore store1;

            if (fromSharded)
            {
                store1 = Sharding.GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_1" });
            }
            else
            {
                store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" });
            }

            using (store1)
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGOR" }, "users/1");
                    await session.SaveChangesAsync();
                }

                store1.Subscriptions.Create(new SubscriptionCreationOptions<User>() { Name = "sub1" });
                store1.Subscriptions.Create(new SubscriptionCreationOptions<User>() { Name = "sub2" });
                store1.Subscriptions.Create(new SubscriptionCreationOptions<User>());

                var states = store1.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(3, states.Count);

                var config = Backup.CreateBackupConfiguration(backupPath);
                var result = await store1.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var backupTaskId = result.TaskId;
                var op = await store1.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupTaskId));
                await op.WaitForCompletionAsync(_reasonableWaitTime);
                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                DocumentStore store2;
                string[] dirs = null;
                if (fromSharded)
                {
                    store2 = GetDocumentStore(new Options { ModifyDatabaseName = s => databaseName });
                    dirs = Directory.GetDirectories(backupPath);
                    Assert.Equal(3, dirs.Length);
                }
                else
                {
                    store2 = Sharding.GetDocumentStore(new Options { ModifyDatabaseName = s => databaseName });
                    dirs = Directory.GetDirectories(backupPath);
                    Assert.Equal(1, dirs.Length);
                }

                var importOptions = new DatabaseSmugglerImportOptions();
                foreach (var dir in dirs)
                {
                    await store2.Smuggler.ImportIncrementalAsync(importOptions, dir);
                    if (fromSharded)
                    {
                        importOptions.OperateOnTypes &= ~DatabaseSmugglerOptions.OperateOnFirstShardOnly;
                    }
                }

                states = await store2.Subscriptions.GetSubscriptionsAsync(0, 10, databaseName);

                Assert.Equal(3, states.Count);
                Assert.True(states.Any(x => x.SubscriptionName.Equals("sub1")));
                Assert.True(states.Any(x => x.SubscriptionName.Equals("sub2")));

                var mre = new AsyncManualResetEvent();
                using (var worker = store2.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("sub1")
                {
                    MaxDocsPerBatch = 5,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1)
                }))
                {
                    var t = worker.Run(_ =>
                    {
                        mre.Set();
                    });

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                }
            }
        }

        private class Dog
        {
            public string Name;
            public string Owner;
        }
    }
}
