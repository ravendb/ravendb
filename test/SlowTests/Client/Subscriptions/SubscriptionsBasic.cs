using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client.Subscriptions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.Issues.RavenDB_8450;
using DisposableAction = Voron.Util.DisposableAction;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace SlowTests.Client.Subscriptions
{
    public class SubscriptionsBasic : SubscriptionTestBase
    {
        public SubscriptionsBasic(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

        [RavenFact(RavenTestCategory.Subscriptions)]
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

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task SubscriptionsBatchSizeShouldIgnoreSkippedItems()
        {
            using (var store = GetDocumentStore())
            {
                var sub = store.Subscriptions.Create(new SubscriptionCreationOptions<User>
                {
                    Filter = user => user.Count > 0
                });

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(
                    new SubscriptionWorkerOptions(sub)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                        MaxDocsPerBatch = 2
                    });

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User { Count = 1 });
                    }
                    session.SaveChanges();
                }

                _ = subscription.Run(async x =>
                {
                    using var session = x.OpenAsyncSession();

                    foreach (var item in x.Items)
                    {
                        item.Result.Count--;
                    }

                    await session.SaveChangesAsync();
                });

                await AssertWaitForCountAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        return await session.Query<User>().Where(u => u.Count > 0).ToListAsync();
                    }
                }, 0);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.BackupExportImport)]
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

                using (Backup.RestoreDatabase(store,
                           new RestoreBackupConfiguration { BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = databaseName }))
                {
                    using (var store2 = GetDocumentStore(new Options { ModifyDatabaseName = s => databaseName, CreateDatabase = false }))
                    {
                        subscriptionStataList = store2.Subscriptions.GetSubscriptions(0, 10, databaseName);

                        Assert.Equal(3, subscriptionStataList.Count);
                        Assert.True(subscriptionStataList.Any(x => x.SubscriptionName.Equals("sub1")));
                        Assert.True(subscriptionStataList.Any(x => x.SubscriptionName.Equals("sub2")));

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
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.BackupExportImport)]
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

                    using (var session = store2.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "oren" }, "users/1");
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

        [RavenFact(RavenTestCategory.Subscriptions)]
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

        [RavenFact(RavenTestCategory.Subscriptions)]
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

                    SubscriptionState subscriptionState;
                    using (database.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        subscriptionState = database.SubscriptionStorage.GetSubscriptionByName(context, subscriptionId);
                    }
                    var index = database.SubscriptionStorage.PutSubscription(new SubscriptionCreationOptions()
                    {
                        ChangeVector = cvBigger,
                        Name = subscriptionState.SubscriptionName,
                        Query = subscriptionState.Query
                    }, Guid.NewGuid().ToString(), subscriptionState.SubscriptionId, false);

                    await index.WaitWithTimeout(_reasonableWaitTime);

                    await database.RachisLogIndexNotifications.WaitForIndexNotification(index.Result.Item2, database.ServerStore.Engine.OperationTimeout).WaitWithTimeout(_reasonableWaitTime);

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
                            Assert.Fail("Got age " + item.Age);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public void ShouldIncrementFailingTests()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                Cluster.SuspendObserver(Server);

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

        [RavenFact(RavenTestCategory.Subscriptions)]
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
                        switch (x)
                        {
                            case SubscriptionClosedException sce:
                                Assert.True(sce.CanReconnect);
                                Assert.Equal($"Subscription With Id '{state.SubscriptionName}' was closed.  Raven.Client.Exceptions.Documents.Subscriptions.SubscriptionClosedException: The subscription {state.SubscriptionName} query has been modified, connection must be restarted", x.Message);
                                break;
                            case SubscriptionChangeVectorUpdateConcurrencyException:
                                // sometimes we may hit cv concurrency exception because of the update
                                Assert.StartsWith($"Can't acknowledge subscription with name '{state.SubscriptionName}' due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value. Stored value: , received value: A:11", x.Message);
                                break;
                        }
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

                    var db = await Databases.GetDocumentDatabaseInstanceFor(store, store.Database);
                    using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var query = WaitForValue(() =>
                        {
                            var connectionState = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, state.SubscriptionName);
                            return connectionState?.GetConnections().FirstOrDefault()?.SubscriptionState.Query;
                        }, newQuery);

                        Assert.Equal(newQuery, query);
                    }

                    Assert.True(second.Wait(_reasonableWaitTime));
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
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

                var db = await Databases.GetDocumentDatabaseInstanceFor(store, store.Database);
                using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var query = WaitForValue(() =>
                    {
                        var connectionState = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, state.SubscriptionName);

                        return connectionState?.GetConnections().FirstOrDefault()?.SubscriptionState.Query;
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

        [RavenFact(RavenTestCategory.Subscriptions)]
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
                var db = await Databases.GetDocumentDatabaseInstanceFor(store, store.Database);
                using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var query = WaitForValue(() =>
                    {
                        var connectionState = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, state.SubscriptionName);

                        return connectionState?.GetConnections().FirstOrDefault()?.SubscriptionState.Query;
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

        [RavenFact(RavenTestCategory.Subscriptions)]
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
                    Assert.True(ex is DatabaseDoesNotExistException || ex is SubscriptionDoesNotExistException, ex.ToString());
                    Assert.Contains(
                        ex is SubscriptionDoesNotExistException
                            ? $"Stopping subscription '{subscription.SubscriptionName}' on node A, because database '{store.Database}' is being deleted."
                            : $"Database '{store.Database}' does not exist.", ex.Message);
                }

                await t;
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
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

        [RavenFact(RavenTestCategory.Subscriptions)]
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

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.TimeSeries)]
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

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.TimeSeries)]
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

        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.TimeSeries)]
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

        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.TimeSeries)]
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

        [RavenFact(RavenTestCategory.Subscriptions)]
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

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Worker_should_consider_RegisterSubscriptionConnection_time_on_calculation_of_LastConnectionFailure()
        {
            DoNotReuseServer();
            var maxErroneousPeriod = TimeSpan.FromSeconds(1);
            using (var store = GetDocumentStore())
            {
                var id1 = store.Subscriptions.Create(new SubscriptionCreationOptions<Company>());
                var workerOptions1 = new SubscriptionWorkerOptions(id1) { Strategy = SubscriptionOpeningStrategy.WaitForFree, MaxErroneousPeriod = maxErroneousPeriod };

                var worker1Ack = new AsyncManualResetEvent();
                AsyncManualResetEvent worker1Retry = null;

                using var worker1 = store.Subscriptions.GetSubscriptionWorker<Company>(workerOptions1, store.Database);
                worker1.AfterAcknowledgment += _ =>
                {
                    worker1Ack.Set();
                    return Task.CompletedTask;
                };
                worker1.OnSubscriptionConnectionRetry += exception =>
                {
                    worker1Retry?.Set();
                };
                var t1 = worker1.Run(x => { });

                using (var session = store.OpenSession())
                {
                    session.Store(new Company());
                    session.SaveChanges();
                }
                await worker1Ack.WaitAsync(_reasonableWaitTime);

                var worker2Ack = new AsyncManualResetEvent();
                ManualResetEvent worker2Retry = null;
                using var worker2 = store.Subscriptions.GetSubscriptionWorker<Company>(workerOptions1, store.Database);
                AsyncManualResetEvent worker1AfterRegisterSubscriptionConnection = null;
                worker2.OnSubscriptionConnectionRetry += async exception =>
                {
                    worker2Retry?.Set();
                    if (worker1AfterRegisterSubscriptionConnection == null)
                        return;

                    await worker1AfterRegisterSubscriptionConnection.WaitAsync(_reasonableWaitTime);
                };
                worker2.AfterAcknowledgment += _ =>
                {
                    worker2Ack.Set();
                    return Task.CompletedTask;
                };

                var t2 = worker2.Run(x => { });

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var testingStuff = db.ForTestingPurposesOnly();

                var subscriptionInterrupt = new AsyncManualResetEvent();
                using (testingStuff.CallDuringWaitForChangedDocuments(() =>
                       {
                           worker1Retry ??= new AsyncManualResetEvent();
                           subscriptionInterrupt.Set();
                           throw new SubscriptionDoesNotBelongToNodeException($"DROPPED BY TEST") { AppropriateNode = null };
                       }))
                {
                    await subscriptionInterrupt.WaitAsync(_reasonableWaitTime);
                }

                await worker1Retry.WaitAsync(_reasonableWaitTime);
                using (var session = store.OpenSession())
                {
                    session.Store(new Company());
                    session.SaveChanges();
                }

                await worker2Ack.WaitAsync(_reasonableWaitTime);

                var waitedForFreeDuration = (maxErroneousPeriod * 2).Ticks;
                var failed = false;
                worker1AfterRegisterSubscriptionConnection = new AsyncManualResetEvent();
                using (testingStuff.CallAfterRegisterSubscriptionConnection(_ =>
                       {
                           if (worker2Retry.WaitOne(_reasonableWaitTime) == false)
                           {
                               failed = true;
                           }
                           worker1Retry.Reset(true);
                           worker1AfterRegisterSubscriptionConnection.Set();
                           throw new SubscriptionDoesNotBelongToNodeException($"DROPPED BY TEST") { AppropriateNode = null, RegisterConnectionDurationInTicks = waitedForFreeDuration };
                       }))
                {
                    subscriptionInterrupt.Reset(true);
                    using (testingStuff.CallDuringWaitForChangedDocuments(() =>
                           {
                               // drop subscription
                               worker2Retry ??= new ManualResetEvent(false);
                               subscriptionInterrupt.Set();
                               throw new SubscriptionDoesNotBelongToNodeException($"DROPPED BY TEST") { AppropriateNode = null };
                           }))
                    {
                        Assert.True(await subscriptionInterrupt.WaitAsync(_reasonableWaitTime));
                    }

                    await worker1AfterRegisterSubscriptionConnection.WaitAsync(_reasonableWaitTime);
                }
                Assert.False(failed, "failed");

                Assert.True(await worker1Retry.WaitAsync(_reasonableWaitTime));
                Assert.False(t1.IsFaulted);
                Assert.False(t2.IsFaulted);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task EnsureSingleSubscriptionDoesNotContinueBeforeAckSent()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new() { Query = "from Users" });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }

                var holdAck = new AsyncManualResetEvent();
                var tryTriggerNextBatch = new AsyncManualResetEvent();

                var firstCV = "";

                using (var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName) { MaxDocsPerBatch = 1 }))
                {
                    _ = worker.Run(async batch =>
                    {
                        firstCV = batch.Items[0].ChangeVector;
                        tryTriggerNextBatch.Set();
                        await holdAck.WaitAsync();
                    });

                    await tryTriggerNextBatch.WaitAsync();
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    string lastChangeVectorSent = null;
                    Assert.True(await WaitForValueAsync(async () =>
                    {
                        var db = await Databases.GetDocumentDatabaseInstanceFor(store, store.Database);
                        using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var connectionState = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, subscriptionName);
                            if (connectionState == null)
                                return false;

                            lastChangeVectorSent = connectionState.LastChangeVectorSent;

                            return lastChangeVectorSent != null;
                        }
                    }, true, interval: 500));

                    Assert.Equal(firstCV, lastChangeVectorSent); //make sure LastChangeVectorSent didn't advance in server even though there was no ack

                    holdAck.Set();
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task WaitingSubscriptionShouldBeRegisteredInSubscriptionConnections()
        {
            using (var store = GetDocumentStore())
            {
                var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions { Query = "from Users", Name = "Subscription0" });

                List<Task> workerTasks = new List<Task>();
                var finishedWorkersCde = new CountdownEvent(2);

                var mre1 = new AsyncManualResetEvent();
                var mreConnect1 = new AsyncManualResetEvent();
                var mre2 = new AsyncManualResetEvent();
                var mreConnect2 = new AsyncManualResetEvent();
                using var subsWorker1 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subsId)
                {
                    Strategy = SubscriptionOpeningStrategy.WaitForFree
                });
                subsWorker1.OnEstablishedSubscriptionConnection += () =>
                {
                    mreConnect1.Set();
                };
                var t1 = subsWorker1.Run(_ => { mre1.Set(); });
                _ = t1.ContinueWith(res =>
                {
                    finishedWorkersCde.Signal();
                });

                Assert.True(await mreConnect1.WaitAsync(_reasonableWaitTime));

                using var subsWorker2 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subsId)
                {
                    Strategy = SubscriptionOpeningStrategy.WaitForFree
                });
                subsWorker2.OnEstablishedSubscriptionConnection += () =>
                {
                    mreConnect2.Set();
                };
                var t2 = subsWorker2.Run(_ => { mre2.Set(); });
                _ = t2.ContinueWith(res =>
                {
                    finishedWorkersCde.Signal();
                });

                workerTasks.Add(t1);
                workerTasks.Add(t2);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }
                Assert.True(await mre1.WaitAsync(_reasonableWaitTime));

                await AssertRunningSubscriptionAndDrop(store);

                // waiting subscription connects
                Assert.True(await mreConnect2.WaitAsync(_reasonableWaitTime));
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }
                // waiting subscription process document
                Assert.True(await mre2.WaitAsync(_reasonableWaitTime));

                await AssertRunningSubscriptionAndDrop(store);

                // both workers should be disconnected
                Assert.True(finishedWorkersCde.Wait(_reasonableWaitTime));
                Assert.All(workerTasks, task => Assert.True(task.IsCompleted));
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task DatabaseShouldNotGetIdleWhenTHereIsActiveSubscriptionConnection()
        {
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
                }
            });

            using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
            using var dispose = new DisposableAction(() => server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = false);
            server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = true;

            var db = await GetDatabase(server, store.Database);
            var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions { Query = "from Users", Name = Guid.NewGuid().ToString() });
            using var subsWorker1 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subsId));
            var mreConnect1 = new AsyncManualResetEvent();
            subsWorker1.AfterAcknowledgment += _ =>
            {
                mreConnect1.Set();
                return Task.CompletedTask;
            };
            var t1 = subsWorker1.Run(_ => { }).ContinueWith(res => { });

            using (var session = store.OpenSession())
            {
                session.Store(new User(), "Users/1");
                session.SaveChanges();
            }

            Assert.True(await mreConnect1.WaitAsync(_reasonableWaitTime));
            List<SubscriptionState> states;
            var re = store.GetRequestExecutor();
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var cmd = new GetRunningSubscriptionsCommand(0, int.MaxValue);
                await re.ExecuteAsync(cmd, context);
                states = cmd.Result.ToList();
            }
            Assert.Equal(1, states.Count);

            Assert.True(await WaitForValueAsync(async () =>
            {
                var url = Uri.EscapeDataString($"{store.Urls.First()}/admin/debug/databases/idle");
                var response = await re.HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"{store.Urls.First()}/admin/debug/databases/idle").WithConventions(store.Conventions));
                var raw = await response.Content.ReadAsStringAsync();
                var idleDatabaseStatistics = JsonConvert.DeserializeObject<IdleDatabaseStatistics>(raw);
                if (idleDatabaseStatistics == null)
                    return false;
                if (1 != idleDatabaseStatistics.Results.Count)
                    return false;

                var stats = idleDatabaseStatistics.Results.FirstOrDefault();
                if (stats == null)
                    return false;

                if (stats.Explanations.Any(s => s.StartsWith("Cannot unload database because number of Subscriptions connections")))
                    return true;

                return false;
            }, true, timeout: 60000, interval: 1000), $"WaitForValue=>LastRecentlyUsed");
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task ShouldContinueSubscriptionOnClientException()
        {
            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });

            var subscriptionDocuments = await store.Subscriptions.GetSubscriptionsAsync(0, 10);

            Assert.Equal(0, subscriptionDocuments.Count);

            await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

            subscriptionDocuments = await store.Subscriptions.GetSubscriptionsAsync(0, 10);

            Assert.Equal(1, subscriptionDocuments.Count);
            Assert.Equal("from 'Users' as doc", subscriptionDocuments[0].Query);

            using var subscription = store.Subscriptions.GetSubscriptionWorker(
                new SubscriptionWorkerOptions(subscriptionDocuments[0].SubscriptionName) { TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1) });

            server.Dispose();
            var unexpected = new AsyncManualResetEvent();
            var retry = new AsyncManualResetEvent();
            Exception onUnexpectedSubscriptionError = null;
            Exception onSubscriptionConnectionRetry = null;
            subscription.OnUnexpectedSubscriptionError += exception =>
            {
                onUnexpectedSubscriptionError = exception;
                unexpected.Set();
            };
            subscription.OnSubscriptionConnectionRetry += exception =>
            {
                onSubscriptionConnectionRetry = exception;
                retry.Set();
            };
            var t = subscription.Run(x => { });
            Assert.True(await unexpected.WaitAsync(_reasonableWaitTime));
            Assert.True(await retry.WaitAsync(_reasonableWaitTime));
            Assert.False(t.IsFaulted);
            Assert.Equal(onSubscriptionConnectionRetry.GetType().FullName, onUnexpectedSubscriptionError.GetType().FullName);
            Assert.NotNull(onSubscriptionConnectionRetry.InnerException);
            Assert.NotNull(onUnexpectedSubscriptionError.InnerException);
            Assert.Equal(onSubscriptionConnectionRetry.InnerException.GetType().FullName, onUnexpectedSubscriptionError.InnerException.GetType().FullName);
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task DeletingSubscriptionShouldRemoveItsState()
        {
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "1000000",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "1000000",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
                }
            });

            using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
            var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions { Query = "from Users", Name = "Subscription0" });
            var state = await store.Subscriptions.GetSubscriptionStateAsync(name);


            var connections = new CountdownEvent(5);
            var tasks = new List<Task>();
            for (int i = 0; i < 5; i++)
            {
                var subsWorker1 = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(name) { Strategy = SubscriptionOpeningStrategy.Concurrent });
                subsWorker1.OnEstablishedSubscriptionConnection += () => { connections.Signal(); };
                tasks.Add(subsWorker1.Run(_ => { }));
            }

            connections.Wait(_reasonableWaitTime);

            await store.Subscriptions.DeleteAsync(name);

            var db = await Databases.GetDocumentDatabaseInstanceFor(server, store);
            await AssertWaitForExceptionAsync<KeyNotFoundException>(async () => await Task.Run(() => db.SubscriptionStorage.GetSubscriptionStateById(state.SubscriptionId)), interval: 1000);
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Skip = "RavenDB-20024")]
        public void CanGetSubscriptionsResultsWithEscapeHandling(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Arava"
                    }, "people/1");
                    session.Store(new Dog(DateTime.Now)
                    {
                        Name = 322,
                        Owner = "people/1"
                    });
                     session.SaveChanges();
                }

                var result = store.Operations.Send(new SubscriptionTryoutOperation(new SubscriptionTryout
                {
                    Query = "from Dogs include Owner"
                }));

                dynamic obj = JObject.Parse(result);
                Assert.NotNull(obj.Includes);
                Assert.NotNull(obj.Includes["people/1"]);
                Assert.Equal("Arava", obj.Includes["people/1"].Name.ToString());
            }
        }

        private class IdleDatabaseStatistics
        {
            public string MaxIdleTime { get; set; }
            public string FrequencyToCheckForIdle { get; set; }
            public List<DatabasesDebugHandler.IdleDatabaseStatistics> Results { get; set; }
        }

        private class GetRunningSubscriptionsCommand : GetSubscriptionsCommand
        {
            public GetRunningSubscriptionsCommand(int start, int pageSize) : base(start, pageSize)
            {
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var req = base.CreateRequest(ctx, node, out url);
                url = $"{url}&running=true";
                return req;
            }
        }

        private async Task AssertRunningSubscriptionAndDrop(DocumentStore store)
        {
            DocumentDatabase db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var name = $"Subscription0";
                var subscription = db
                    .SubscriptionStorage
                    .GetSubscriptionWithDataByNameFromServerStore(context, name, history: false, running: true);
                Assert.NotNull(subscription);
                db.SubscriptionStorage.DropSubscriptionConnections(subscription.SubscriptionId,
                    new SubscriptionClosedException("Dropped by Test"));
            }
        }

        private class Dog
        {
            public int Name { get; set; }

            public DateTime Zz { get; set; }

            public string Owner { get; set; }

            public Dog(DateTime zz)
            {
                Zz = zz;
            }
        }
    }
}
