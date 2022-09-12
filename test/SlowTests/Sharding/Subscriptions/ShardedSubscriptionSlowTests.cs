using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Server;
using Sparrow.Utils;
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

        [Fact]
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

        [Fact]
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
                using (db.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var query = WaitForValue(() =>
                    {
                        var connectionState = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, state.SubscriptionName);
                        return connectionState?.GetConnections().FirstOrDefault()?.SubscriptionState.Query;
                    }, newQuery);

                    Assert.Equal(newQuery, query);
                }
            }
        }

        [Fact]
        public async Task CanUpdateSubscriptionToStartFromLastDocument()
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
                using var docs = new CountdownEvent(count / 2);

                var flag = true;
                var t = subscription.Run(x =>
                {
                    if (docs.IsSet)
                        flag = false;
                    docs.Signal(x.NumberOfItemsInBatch);
                });

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < count / 2; i++)
                    {

                        session.Store(new User
                        {
                            Name = $"EGR_{i}",
                            Age = 18
                        }, Guid.NewGuid().ToString());
                    }

                    session.SaveChanges();
                }

                Assert.True(docs.Wait(_reasonableWaitTime));

                const string newQuery = "from Users where Age > 18";

                store.Subscriptions.Update(new SubscriptionUpdateOptions
                {
                    Name = state.SubscriptionName,
                    Query = newQuery,
                    ChangeVector = $"{Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument}"
                });

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(newQuery, newState.Query);
                Assert.Equal(state.SubscriptionId, newState.SubscriptionId);

                await CheckSubscriptionNewQuery(store, state, newQuery);

                for (int i = count / 2; i < count; i++)
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

                await Task.Delay(500);
                Assert.True(flag);
                AssertOnSubscriptionConnectionRetryEventException(onSubscriptionConnectionRetryException, state);
            }
        }

        [Fact]
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

        [Fact(Skip = "Cannot set CV by admin in sharded subscription")]
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
                    using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        subscriptionState = Server.ServerStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, store.Database, subscriptionId);
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

        [Theory(Skip = "TODO add include")]
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

        [Fact(Skip = "RavenDB-18568")]
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
                    await AssertNoLeftovers(store, id);
                }
            }
        }

        [Fact(Skip = "RavenDB-18568")]
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
                    await AssertNoLeftovers(store, id);
                }
            }
        }

        private async Task AssertNoLeftovers(IDocumentStore store, string id)
        {
            var shards = Sharding.GetShardsDocumentDatabaseInstancesFor(store);
            await foreach (var db in shards)
            {
                await AssertWaitForValueAsync(() =>
                {
                    using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        return Task.FromResult(db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, id).GetNumberOfResendDocuments(SubscriptionType.Document));
                    }
                }, 0);
            }
        }
    }
}
