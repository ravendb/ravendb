using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client.Subscriptions;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class SubscriptionsBasic : SubscriptionTestBase
    {
        public SubscriptionsBasic(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

        [Fact]
        public void CanGetSubscriptionsFromDatabase()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionDocuments = store.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(0, subscriptionDocuments.Count);

                store.Subscriptions.Create(new SubscriptionCreationOptions<User>());

                subscriptionDocuments = store.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(1, subscriptionDocuments.Count);
                Assert.Equal("from 'Users' as doc", subscriptionDocuments[0].Query);

                var subscription = store.Subscriptions.GetSubscriptionWorker(
                    new SubscriptionWorkerOptions(subscriptionDocuments[0].SubscriptionName)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    });

                var docs = new CountdownEvent(1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                subscription.Run(x => docs.Signal(x.NumberOfItemsInBatch));

                Assert.True(docs.Wait(_reasonableWaitTime));
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanBackupAndRestoreSubscriptions()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                store.Subscriptions.Create(new SubscriptionCreationOptions<User>() { Name = "sub1" });
                store.Subscriptions.Create(new SubscriptionCreationOptions<User>() { Name = "sub2" });
                store.Subscriptions.Create(new SubscriptionCreationOptions<User>());

                var subscriptionStataList = store.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(3, subscriptionStataList.Count);

                var config = Backup.CreateBackupConfiguration(backupPath);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = databaseName
                }))
                {
                    subscriptionStataList = store.Subscriptions.GetSubscriptions(0, 10, databaseName);

                    Assert.Equal(3, subscriptionStataList.Count);
                    Assert.True(subscriptionStataList.Any(x => x.SubscriptionName.Equals("sub1")));
                    Assert.True(subscriptionStataList.Any(x => x.SubscriptionName.Equals("sub2")));
                }
            }
        }

        [Fact]
        public async Task CanExportAndImportSubscriptions()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_1",
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_2"
                }))
                {
                    store1.Subscriptions.Create(new SubscriptionCreationOptions<User>() { Name = "sub1" });
                    store1.Subscriptions.Create(new SubscriptionCreationOptions<User>() { Name = "sub2" });
                    store1.Subscriptions.Create(new SubscriptionCreationOptions<User>());

                    var subscriptionStataList = store1.Subscriptions.GetSubscriptions(0, 10);

                    Assert.Equal(3, subscriptionStataList.Count);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    subscriptionStataList = store2.Subscriptions.GetSubscriptions(0, 10, store2.Database);

                    Assert.Equal(3, subscriptionStataList.Count);
                    Assert.True(subscriptionStataList.Any(x => x.SubscriptionName.Equals("sub1")));
                    Assert.True(subscriptionStataList.Any(x => x.SubscriptionName.Equals("sub2")));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public void CanUseNestedPropertiesInSubscriptionCriteria()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new PersonWithAddress
                        {
                            Address = new Address()
                            {
                                Street = "1st Street",
                                ZipCode = i % 2 == 0 ? 999 : 12345
                            }
                        });

                        session.Store(new PersonWithAddress
                        {
                            Address = new Address()
                            {
                                Street = "2nd Street",
                                ZipCode = 12345
                            }
                        });

                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }
                store.Subscriptions.Create<User>();

                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<PersonWithAddress>()
                {
                    Filter = x => x.Address.Street == "1st Street" && x.Address.ZipCode != 999
                });

                using (var carolines = store.Subscriptions.GetSubscriptionWorker<PersonWithAddress>(new SubscriptionWorkerOptions(id)
                {
                    MaxDocsPerBatch = 5,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var docs = new CountdownEvent(5);
                    var t = carolines.Run(x =>
                    {
                        foreach (var user in x.Items)
                        {
                            Assert.Equal("1st Street", user.Result.Address.Street);
                        }
                        docs.Signal(x.NumberOfItemsInBatch);
                    });

                    try
                    {
                        Assert.True(docs.Wait(_reasonableWaitTime));
                    }
                    catch
                    {
                        if (t.IsFaulted)
                            t.Wait();
                        throw;
                    }
                }
            }
        }

        [Fact]
        public async Task RunningSubscriptionShouldJumpToNextChangeVectorIfItWasChangedByAdmin()
        {
            using (var store = GetDocumentStore())
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
                    var database = await GetDatabase(store.Database);

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
                    var t = subscription.Run(x => x.Items.ForEach(i => users.Add(i.Result)));

                    var firstItemchangeVector = cvFirst.ToChangeVector();
                    firstItemchangeVector[0].Etag += 10;
                    cvBigger = firstItemchangeVector.SerializeVector();

                    Assert.True(await ackFirstCV.WaitAsync(_reasonableWaitTime));

                    SubscriptionStorage.SubscriptionGeneralDataAndStats subscriptionState;
                    using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        subscriptionState = database.SubscriptionStorage.GetSubscriptionFromServerStore(context, subscriptionId);
                    }
                    var index = database.SubscriptionStorage.PutSubscription(new SubscriptionCreationOptions()
                    {
                        ChangeVector = cvBigger,
                        Name = subscriptionState.SubscriptionName,
                        Query = subscriptionState.Query
                    }, Guid.NewGuid().ToString(), subscriptionState.SubscriptionId, false);

                    await index.WaitWithTimeout(_reasonableWaitTime);

                    await database.RachisLogIndexNotifications.WaitForIndexNotification(index.Result, database.ServerStore.Engine.OperationTimeout).WaitWithTimeout(_reasonableWaitTime);

                    using (var session = store.OpenSession())
                    {
                        for (var i = 0; i < 20; i++)
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

        [Fact]
        public void ShouldIncrementFailingTests()
        {
            using (var store = GetDocumentStore())
            {
                Server.ServerStore.Observer.Suspended = true;
                var lastId = string.Empty;
                var docsAmount = 50;
                using (var biPeople = store.BulkInsert())
                {
                    for (int i = 0; i < docsAmount; i++)
                    {
                        lastId = biPeople.Store(new Company
                        {
                            Name = "Something Inc. #" + i
                        });
                    }
                }
                string lastChangeVector;
                using (var session = store.OpenSession())
                {
                    var lastCompany = session.Load<Company>(lastId);
                    lastChangeVector = session.Advanced.GetMetadataFor(lastCompany)[Raven.Client.Constants.Documents.Metadata.ChangeVector].ToString();
                }

                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<Company>());

                var subscription = store.Subscriptions.GetSubscriptionWorker<Company>(new SubscriptionWorkerOptions(id)
                {
                    MaxDocsPerBatch = 1,
                    IgnoreSubscriberErrors = true,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                var cde = new CountdownEvent(docsAmount);

                subscription.Run(x =>
                {
                    throw new Exception();
                });

                subscription.AfterAcknowledgment += processed =>
                {
                    cde.Signal(processed.NumberOfItemsInBatch);
                    return Task.CompletedTask;
                };
                Assert.True(cde.Wait(_reasonableWaitTime));

                var subscriptionStatus = store.Subscriptions.GetSubscriptions(0, 1024).ToList();

                Assert.Equal(subscriptionStatus[0].ChangeVectorForNextBatchStartingPoint, lastChangeVector);
            }
        }

        [Fact]
        public async Task CanUpdateSubscriptionToStartFromBeginningOfTime()
        {
            using (var store = GetDocumentStore())
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
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(16)
                }))
                {
                    subscription.OnSubscriptionConnectionRetry += x =>
                    {
                        var sce = x as SubscriptionClosedException;
                        Assert.NotNull(sce);
                        Assert.Equal(typeof(SubscriptionClosedException), x.GetType());
                        Assert.True(sce.CanReconnect);
                        Assert.Equal($"Subscription With Id '{state.SubscriptionName}' was closed.  Raven.Client.Exceptions.Documents.Subscriptions.SubscriptionClosedException: The subscription {state.SubscriptionName} query has been modified, connection must be restarted", x.Message);
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
                            });
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

                    var db = await GetDocumentDatabaseInstanceFor(store, store.Database);
                    using (db.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var query = WaitForValue(() =>
                        {
                            var connectionState = db.SubscriptionStorage.GetSubscriptionConnection(ctx, state.SubscriptionName);
                            return connectionState?.Connection?.SubscriptionState.Query;
                        }, newQuery);

                        Assert.Equal(newQuery, query);
                    }

                    Assert.True(second.Wait(_reasonableWaitTime));
                }
            }
        }

        [Fact]
        public async Task CanUpdateSubscriptionToStartFromLastDocument()
        {
            using (var store = GetDocumentStore())
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
                subscription.OnSubscriptionConnectionRetry += x =>
                {
                    var sce = x as SubscriptionClosedException;
                    Assert.NotNull(sce);
                    Assert.Equal(typeof(SubscriptionClosedException), x.GetType());
                    Assert.True(sce.CanReconnect);
                    Assert.Equal($"Subscription With Id '{state.SubscriptionName}' was closed.  Raven.Client.Exceptions.Documents.Subscriptions.SubscriptionClosedException: The subscription {state.SubscriptionName} query has been modified, connection must be restarted", x.Message);
                };
                using var docs = new CountdownEvent(count / 2);

                var flag = true;
                var t = subscription.Run(x =>
                {
                    if (docs.IsSet)
                        flag = false;
                    docs.Signal(x.NumberOfItemsInBatch);
                });

                for (int i = 0; i < count / 2; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = $"EGR_{i}",
                            Age = 18
                        });
                        session.SaveChanges();
                    }
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

                var db = await GetDocumentDatabaseInstanceFor(store, store.Database);
                using (db.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var query = WaitForValue(() =>
                    {
                        var connectionState = db.SubscriptionStorage.GetSubscriptionConnection(ctx, state.SubscriptionName);

                        return connectionState?.Connection?.SubscriptionState.Query;
                    }, newQuery);

                    Assert.Equal(newQuery, query);
                }

                for (int i = count / 2; i < count; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = $"EGR_{i}",
                            Age = 18
                        });
                        session.SaveChanges();
                    }
                }

                await Task.Delay(500);
                Assert.True(flag);
            }
        }

        [Fact]
        public async Task CanUpdateSubscriptionToStartFromDoNotChange()
        {
            using (var store = GetDocumentStore())
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
                subscription.OnSubscriptionConnectionRetry += x =>
                {
                    var sce = x as SubscriptionClosedException;
                    Assert.NotNull(sce);
                    Assert.Equal(typeof(SubscriptionClosedException), x.GetType());
                    Assert.True(sce.CanReconnect);
                    Assert.Equal($"Subscription With Id '{state.SubscriptionName}' was closed.  Raven.Client.Exceptions.Documents.Subscriptions.SubscriptionClosedException: The subscription {state.SubscriptionName} query has been modified, connection must be restarted", x.Message);
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
                        });
                        session.SaveChanges();
                    }
                }

                WaitForValue(() => docs.CurrentCount, count / 2);

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
                var db = await GetDocumentDatabaseInstanceFor(store, store.Database);
                using (db.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var query = WaitForValue(() =>
                    {
                        var connectionState = db.SubscriptionStorage.GetSubscriptionConnection(ctx, state.SubscriptionName);

                        return connectionState?.Connection?.SubscriptionState.Query;
                    }, newQuery);

                    Assert.Equal(newQuery, query);
                }

                for (int i = 0; i < count / 2; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = $"EGR_{i}",
                            Age = 19
                        });
                        session.SaveChanges();
                    }
                }

                Assert.True(docs.Wait(_reasonableWaitTime));
            }
        }

        [Fact]
        public async Task AcknowledgeSubscriptionBatchWhenDBisBeingDeletedShouldThrow()
        {
            using var store = GetDocumentStore();

            var id = await store.Subscriptions.CreateAsync<User>();
            using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "EGR",
                        Age = 39
                    });
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
                            ? $"Stopping subscription '{subscription.SubscriptionName}' on node A, because database '{store.Database}' is being deleted."
                            : $"Database '{store.Database}' does not exist.", ex.Message);
                }

                await t;
            }
        }

        [Fact]
        public async Task ShouldThrowWhenCannotDeserializeEntity()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions() { Query = @"from Dogs" });

                using (var commands = store.Commands())
                {
                    Dog dog = new Dog(DateTime.UtcNow) { Name = 1 };
                    var str = JsonSerializer.Serialize<Dog>(dog);
                    dynamic json = JObject.Parse(str);
                    json.Name = "a";
                    await commands.PutAsync("dog/1", null, json, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Dogs"}
                    });
                }

                var subscription = store.Subscriptions.GetSubscriptionWorker<Dog>(subscriptionName);
                await Assert.ThrowsAsync<SubscriptionClosedException>(() => subscription.Run(x => { }).WaitAsync(_reasonableWaitTime));
            }
        }

        [Fact]
        public async Task ShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions() { Query = @"from Dogs" });
                var mre = new AsyncManualResetEvent();
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Dog(DateTime.Now) { Name = 1 });
                    await session.SaveChangesAsync();
                }

                var subscription = store.Subscriptions.GetSubscriptionWorker<Dog>(subscriptionName);
                try
                {
                    subscription.ForTestingPurposesOnly().SimulateUnexpectedException = true;

                    subscription.OnUnexpectedSubscriptionError += x =>
                    {
                        mre.Set();
                    };
                    var t = subscription.Run(x => { });

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                }
                finally
                {
                    subscription._forTestingPurposes = null;
                }
            }
        }

        [Fact]
        public void CanCreateSubscriptionWithIncludeTimeSeries_LastRangeByTime()
        {
            var now = DateTime.UtcNow.EnsureMilliseconds();

            using (var store = GetDocumentStore())
            {
                var name = store.Subscriptions
                    .Create(new SubscriptionCreationOptions<Company>()
                    {
                        Includes = builder => builder
                            .IncludeTimeSeries("StockPrice", TimeSeriesRangeType.Last, TimeValue.FromMonths(1))
                    });

                var mre = new ManualResetEventSlim();
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(name);
                worker.Run(batch =>
                {
                    using (var session = batch.OpenSession())
                    {
                        Assert.Equal(0, session.Advanced.NumberOfRequests);

                        var company = session.Load<Company>("companies/1");
                        Assert.Equal(0, session.Advanced.NumberOfRequests);

                        var timeSeries = session.TimeSeriesFor(company, "StockPrice");
                        var timeSeriesEntries = timeSeries.Get(from: now.AddDays(-7));

                        Assert.Equal(1, timeSeriesEntries.Length);
                        Assert.Equal(now, timeSeriesEntries[0].Timestamp);
                        Assert.Equal(10, timeSeriesEntries[0].Value);

                        Assert.Equal(0, session.Advanced.NumberOfRequests);
                    }

                    mre.Set();
                });

                using (var session = store.OpenSession())
                {
                    var company = new Company { Id = "companies/1", Name = "HR" };
                    session.Store(company);

                    session.TimeSeriesFor(company, "StockPrice").Append(now, 10);

                    session.SaveChanges();
                }

                Assert.True(mre.Wait(TimeSpan.FromSeconds(30)));
            }
        }

        [Fact]
        public void CanCreateSubscriptionWithIncludeTimeSeries_LastRangeByCount()
        {
            var now = DateTime.UtcNow.EnsureMilliseconds();

            using (var store = GetDocumentStore())
            {
                var name = store.Subscriptions
                    .Create(new SubscriptionCreationOptions<Company>()
                    {
                        Includes = builder => builder
                            .IncludeTimeSeries("StockPrice", TimeSeriesRangeType.Last, count: 32)
                    });

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
                    }

                    mre.Set();
                });

                using (var session = store.OpenSession())
                {
                    var company = new Company { Id = "companies/1", Name = "HR" };
                    session.Store(company);

                    session.TimeSeriesFor(company, "StockPrice").Append(now.AddDays(-7), 10);

                    session.SaveChanges();
                }

                var result = WaitForValue(() => mre.Wait(TimeSpan.FromSeconds(500)), true);
                if (result == false && t.IsFaulted)
                    Assert.True(result, $"t.IsFaulted: {t.Exception}, {t.Exception?.InnerException}");

                Assert.True(result);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void CanCreateSubscriptionWithIncludeTimeSeries_Array_LastRange(bool byTime)
        {
            var now = DateTime.UtcNow.EnsureMilliseconds();

            using (var store = GetDocumentStore())
            {
                string name;
                if (byTime)
                {
                    name = store.Subscriptions
                        .Create(new SubscriptionCreationOptions<Company>()
                        {
                            Includes = builder => builder
                                .IncludeTimeSeries(new[] { "StockPrice", "StockPrice2" }, TimeSeriesRangeType.Last, TimeValue.FromDays(7))
                        });
                }
                else
                {
                    name = store.Subscriptions
                        .Create(new SubscriptionCreationOptions<Company>()
                        {
                            Includes = builder => builder
                                .IncludeTimeSeries(new[] { "StockPrice", "StockPrice2" }, TimeSeriesRangeType.Last, count: 32)
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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void CanCreateSubscriptionWithIncludeTimeSeries_All_LastRange(bool byTime)
        {
            var now = DateTime.UtcNow.EnsureMilliseconds();

            using (var store = GetDocumentStore())
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

        [Fact]
        public async Task DisposeSubscriptionWorkerShouldNotThrow()
        {
            var mre = new AsyncManualResetEvent();
            var mre2 = new AsyncManualResetEvent();
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = s =>
                {
                    s.OnBeforeRequest += async (sender, args) =>
                    {
                        if (args.Url.Contains("info/remote-task/tcp?database="))
                        {
                            mre.Set();
                            await mre2.WaitAsync(_reasonableWaitTime);
                        }
                    };
                }
            }))
            {
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<Company>());
                var workerOptions = new SubscriptionWorkerOptions(id) { IgnoreSubscriberErrors = true, Strategy = SubscriptionOpeningStrategy.TakeOver };
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(workerOptions, store.Database);

                var t = worker.Run(x => { });

                await mre.WaitAsync(_reasonableWaitTime);
                await worker.DisposeAsync(false);
                mre2.Set();

                var status = WaitForValue(() => t.Status, TaskStatus.RanToCompletion);
                Assert.Equal(TaskStatus.RanToCompletion, status);
                Assert.True(t.IsCompletedSuccessfully, "t.IsCompletedSuccessfully");
            }
        }

        private class Dog
        {
            public int Name { get; set; }

            public DateTime Zz { get; set; }

            public Dog(DateTime zz)
            {
                Zz = zz;
            }
        }
    }
}
