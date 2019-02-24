using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class SubscriptionsBasic : SubscriptionTestBase
    {
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
                Assert.Equal("from Users as doc", subscriptionDocuments[0].Query);

                var subscription = store.Subscriptions.GetSubscriptionWorker(
                    new SubscriptionWorkerOptions(subscriptionDocuments[0].SubscriptionName) {
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

        [Fact]
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

                store.Subscriptions.Create(new SubscriptionCreationOptions<User>(){Name = "sub1"});
                store.Subscriptions.Create(new SubscriptionCreationOptions<User>() { Name = "sub2" });
                store.Subscriptions.Create(new SubscriptionCreationOptions<User>());

                var subscriptionStataList = store.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(3, subscriptionStataList.Count);

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (RestoreDatabase(store, new RestoreBackupConfiguration
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
                    }, subscriptionState.SubscriptionId, false);

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
    }
}
