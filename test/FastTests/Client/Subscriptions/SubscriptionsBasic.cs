using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Platform;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Subscriptions
{
    public class SubscriptionsBasic : RavenTestBase
    {
        public SubscriptionsBasic(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void SubscriptionLongName(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Assert.Throws<SubscriptionNameException>(() => store.Subscriptions.Create<User>(new SubscriptionCreationOptions<User>
                {
                    Name = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                }));
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanDeleteSubscription(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var id1 = store.Subscriptions.Create<User>();
                var id2 = store.Subscriptions.Create<User>();

                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);

                Assert.Equal(2, subscriptions.Count);

                store.Subscriptions.Delete(id1);
                store.Subscriptions.Delete(id2);

                subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);

                Assert.Equal(0, subscriptions.Count);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldThrowWhenOpeningNoExisingSubscription(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions("1")
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });
                var ex = await Assert.ThrowsAsync<SubscriptionDoesNotExistException>(() => subscription.Run(x => { }));
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldThrowOnAttemptToOpenAlreadyOpenedSubscription(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var id = store.Subscriptions.Create<User>();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User());
                        session.SaveChanges();
                    }

                    var amre = new AsyncManualResetEvent();
                    var t = subscription.Run(x => amre.Set());

                    await amre.WaitAsync(_reasonableWaitTime);

                    using (var secondSubscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                    {
                        Strategy = SubscriptionOpeningStrategy.OpenIfFree,
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                    {
                        Assert.True(await Assert.ThrowsAsync<SubscriptionInUseException>(() => secondSubscription.Run(x => { })).WaitWithoutExceptionAsync(_reasonableWaitTime));
                    }
                }
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Subscriptions, RavenPlatform.Windows | RavenPlatform.Linux, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldBeAbleToChangeBufferSizes(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Age = 31 }, "users/1");
                    session.Store(new User { Age = 27 }, "users/12");
                    session.Store(new User { Age = 25 }, "users/3");

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create<User>();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    SendBufferSizeInBytes = 4 * 1024,
                    ReceiveBufferSizeInBytes = 3 * 1024
                }))
                {
                    var users = new BlockingCollection<User>();

                    subscription.Run(batch =>
                    {
                        batch.Items.ForEach(x => users.Add(x.Result));
                    });

                    var expected = new Dictionary<string, int>
                    {
                        ["users/1"] = 31,
                        ["users/12"] = 27,
                        ["users/3"] = 25
                    };
                    Assert.True(users.TryTake(out User user, _reasonableWaitTime));
                    Assert.True(expected.Remove(user.Id, out var age), $"missing {user.Id}");
                    Assert.Equal(age, user.Age);

                    Assert.True(users.TryTake(out user, _reasonableWaitTime));
                    Assert.True(expected.Remove(user.Id, out age), $"missing {user.Id}");
                    Assert.Equal(age, user.Age);

                    Assert.True(users.TryTake(out user, _reasonableWaitTime));
                    Assert.True(expected.Remove(user.Id, out age), $"missing {user.Id}");
                    Assert.Equal(age, user.Age);

                    var expectedSendBufferSize = 4 * 1024;
                    var expectedReceiveBufferSize = 3 * 1024;
                    if (PlatformDetails.RunningOnLinux)
                    {
                        // linux is doubling that value by design
                        expectedSendBufferSize *= 2;
                        expectedReceiveBufferSize *= 2;
                    }

                    Assert.Equal(expectedSendBufferSize, subscription._tcpClient.SendBufferSize);
                    Assert.Equal(expectedReceiveBufferSize, subscription._tcpClient.ReceiveBufferSize);
                }
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Subscriptions, RavenPlatform.Windows | RavenPlatform.Linux, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldStreamAllDocumentsAfterSubscriptionCreation(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Age = 31 }, "users/1");
                    session.Store(new User { Age = 27 }, "users/12");
                    session.Store(new User { Age = 25 }, "users/3");

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create<User>();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var users = new BlockingCollection<User>();

                    subscription.Run(batch =>
                    {
                        batch.Items.ForEach(x => users.Add(x.Result));
                    });

                    var expected = new Dictionary<string, int>
                    {
                        ["users/1"] = 31,
                        ["users/12"] = 27,
                        ["users/3"] = 25
                    };
                    Assert.True(users.TryTake(out User user, _reasonableWaitTime));
                    Assert.True(expected.Remove(user.Id, out var age), $"missing {user.Id}");
                    Assert.Equal(age, user.Age);

                    Assert.True(users.TryTake(out user, _reasonableWaitTime));
                    Assert.True(expected.Remove(user.Id, out age), $"missing {user.Id}");
                    Assert.Equal(age, user.Age);

                    Assert.True(users.TryTake(out user, _reasonableWaitTime));
                    Assert.True(expected.Remove(user.Id, out age), $"missing {user.Id}");
                    Assert.Equal(age, user.Age);

                    var expectedSendBufferSize = SubscriptionWorkerOptions.DefaultSendBufferSizeInBytes;
                    var expectedReceiveBufferSize = SubscriptionWorkerOptions.DefaultReceiveBufferSizeInBytes;
                    if (PlatformDetails.RunningOnLinux)
                    {
                        // linux is doubling that value by design
                        expectedSendBufferSize *= 2;
                        expectedReceiveBufferSize *= 2;
                    }

                    Assert.Equal(expectedSendBufferSize, subscription._tcpClient.SendBufferSize);
                    Assert.Equal(expectedReceiveBufferSize, subscription._tcpClient.ReceiveBufferSize);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldSendAllNewAndModifiedDocs(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var id = store.Subscriptions.Create<User>();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var names = new BlockingCollection<string>();

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "James"
                        }, "users/1");
                        session.SaveChanges();
                    }

                    subscription.Run(batch => batch.Items.ForEach(x => names.Add(x.Result.Name)));

                    string name;
                    Assert.True(names.TryTake(out name, _reasonableWaitTime));
                    Assert.Equal("James", name);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "Adam"
                        }, "users/12");
                        session.SaveChanges();
                    }

                    Assert.True(names.TryTake(out name, _reasonableWaitTime));
                    Assert.Equal("Adam", name);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "David"
                        }, "users/1");
                        session.SaveChanges();
                    }

                    Assert.True(names.TryTake(out name, _reasonableWaitTime));
                    Assert.Equal("David", name);

                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldRespectMaxDocCountInBatch(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create<Company>();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    MaxDocsPerBatch = 25,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }
                ))
                {
                    var cd = new CountdownEvent(100);

                    var t = subscription.Run(batch =>
                    {
                        cd.Signal(batch.NumberOfItemsInBatch);
                        Assert.True(batch.NumberOfItemsInBatch <= 25);
                    });

                    try
                    {
                        Assert.True(cd.Wait(_reasonableWaitTime));
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

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldRespectCollectionCriteria(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.Store(new Company());
                        session.Store(new User());
                    }

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create<User>();

                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    MaxDocsPerBatch = 31,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var docs = new CountdownEvent(100);
                    subscription.Run(batch => docs.Signal(batch.NumberOfItemsInBatch));
                    Assert.True(docs.Wait(_reasonableWaitTime));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, Skip = "RavenDB-8404, RavenDB-8682")]
        public void ShouldRespectStartsWithCriteria(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.Store(new User(), i % 2 == 0 ? "users/" : "users/favorite/");
                    }

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = @"From Users as u
                              Where startsWith(id(u),'users/favorite/)"
                });

                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    MaxDocsPerBatch = 15,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {

                    var docs = new CountdownEvent(50);
                    var t = subscription.Run(batch =>
                    {
                        foreach (var item in batch.Items)
                        {
                            Assert.True(item.Id.StartsWith("users/favorite/"));
                        }
                        docs.Signal(batch.NumberOfItemsInBatch);
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

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void WillAcknowledgeEmptyBatches(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var subscriptionDocuments = store.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(0, subscriptionDocuments.Count);

                var allId = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                using (var allSubscription = store.Subscriptions.GetSubscriptionWorker(allId))
                {
                    var allDocs = new CountdownEvent(500);


                    var filteredUsersId = store.Subscriptions.Create(new SubscriptionCreationOptions
                    {
                        Query = @"from Users where Age <0"
                    });
                    using (var filteredUsersSubscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(filteredUsersId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                    {
                        var usersDocs = new CountdownEvent(1);

                        using (var session = store.OpenSession())
                        {
                            for (int i = 0; i < 500; i++)
                                session.Store(new User(), "another/");
                            session.SaveChanges();
                        }

                        allSubscription.Run(x => allDocs.Signal(x.NumberOfItemsInBatch));

                        filteredUsersSubscription.Run(x => usersDocs.Signal(x.NumberOfItemsInBatch));

                        Assert.True(allDocs.Wait(_reasonableWaitTime));
                        Assert.False(usersDocs.Wait(0));
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldKeepPullingDocsAfterServerRestart(Options options)
        {
            var dataPath = NewDataPath();

            IDocumentStore store = null;
            RavenServer server = null;
            SubscriptionWorker<dynamic> subscriptionWorker = null;
            try
            {
                var co = new ServerCreationOptions
                {
                    RunInMemory = false,
                    CustomSettings = new Dictionary<string, string>()
                    {
                        [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = dataPath
                    },
                    RegisterForDisposal = false
                };

                server = GetNewServer(co);
                options.Server = server;
                store = GetDocumentStore(options);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.Store(new User());
                    session.Store(new User());
                    session.Store(new User());

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());

                subscriptionWorker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1),
                    MaxDocsPerBatch = 1
                });


                var gotBatch = new AsyncManualResetEvent();
                var gotArek = new AsyncManualResetEvent();
                var t = subscriptionWorker.Run(x =>
                {
                    gotBatch.Set();

                    foreach (var item in x.Items)
                    {
                        if (item.Id == "users/arek")
                            gotArek.Set();
                    }
                });

                Assert.True(await gotBatch.WaitAsync(_reasonableWaitTime));

                server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                for (int i = 0; i < 150; i++)
                {
                    try
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(new User(), "users/arek");
                            session.SaveChanges();
                        }
                        break;
                    }
                    catch
                    {
                        Thread.Sleep(25);
                        if (i > 100)
                            throw;
                    }
                }

                Assert.True(await gotArek.WaitAsync(_reasonableWaitTime));
            }
            finally
            {
                subscriptionWorker?.Dispose();
                store?.Dispose();
                server?.Dispose();
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanReleaseSubscription(Options options)
        {
            SubscriptionWorker<dynamic> subscriptionWorker = null;
            SubscriptionWorker<dynamic> throwingSubscriptionWorker = null;
            SubscriptionWorker<dynamic> notThrowingSubscriptionWorker = null;

            DoNotReuseServer();
            var store = GetDocumentStore(options);
            try
            {
                Cluster.SuspendObserver(Server);

                var id = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());
                subscriptionWorker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.OpenIfFree,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });
                var mre = new AsyncManualResetEvent();
                PutUserDoc(store);
                var t = subscriptionWorker.Run(x =>
                {
                    mre.Set();
                });
                Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                mre.Reset();

                throwingSubscriptionWorker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.OpenIfFree,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });
                var subscriptionTask = throwingSubscriptionWorker.Run(x => { });

                Assert.True(await Assert.ThrowsAsync<SubscriptionInUseException>(() => subscriptionTask).WaitWithoutExceptionAsync(_reasonableWaitTime));

                await store.Subscriptions.DropConnectionAsync(id);

                notThrowingSubscriptionWorker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    Strategy = SubscriptionOpeningStrategy.WaitForFree
                });

                t = notThrowingSubscriptionWorker.Run(x =>
                {
                    mre.Set();
                });

                PutUserDoc(store);

                Assert.True(await mre.WaitAsync(_reasonableWaitTime));
            }
            finally
            {
                subscriptionWorker?.Dispose();
                throwingSubscriptionWorker?.Dispose();
                notThrowingSubscriptionWorker?.Dispose();
                store.Dispose();
            }
        }

        private static void PutUserDoc(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User());
                session.SaveChanges();
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldPullDocumentsAfterBulkInsert(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {

                    var docs = new BlockingCollection<User>();

                    using (var bulk = store.BulkInsert())
                    {
                        bulk.Store(new User());
                        bulk.Store(new User());
                        bulk.Store(new User());
                    }

                    subscription.Run(x => x.Items.ForEach(i => docs.Add(i.Result)));

                    Assert.True(docs.TryTake(out _, _reasonableWaitTime));
                    Assert.True(docs.TryTake(out _, _reasonableWaitTime));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, Skip = "RavenDB-15919, need to change the test, since we update the ChangeVectorForNextBatchStartingPoint upon fetching and not acking")]
        public async Task ShouldStopPullingDocsAndCloseSubscriptionOnSubscriberErrorByDefault(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    PutUserDoc(store);
                    var subscriptionTask = subscription.Run(x => throw new Exception("Fake exception"));

                    await Assert.ThrowsAsync<SubscriberErrorException>(() => subscriptionTask.WaitAsync(_reasonableWaitTime));

                    Assert.Equal("Fake exception", subscriptionTask.Exception.InnerExceptions[0].InnerException.Message);
                    var subscriptionConfig = store.Subscriptions.GetSubscriptions(0, 1).First();
                    Assert.True(string.IsNullOrEmpty(subscriptionConfig.ChangeVectorForNextBatchStartingPoint));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanSetToIgnoreSubscriberErrors(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    IgnoreSubscriberErrors = true,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var docs = new BlockingCollection<User>();

                    PutUserDoc(store);
                    PutUserDoc(store);

                    var subscriptionTask = subscription.Run(x =>
                    {
                        x.Items.ForEach(i => docs.Add(i.Result));
                        throw new Exception("Fake exception");
                    });

                    Assert.True(docs.TryTake(out _, _reasonableWaitTime));
                    Assert.True(docs.TryTake(out _, _reasonableWaitTime));
                    Assert.Null(subscriptionTask.Exception);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Skip = "https://issues.hibernatingrhinos.com/issue/RavenDB-21581")]
        public async Task RavenDB_3452_ShouldStopPullingDocsIfReleased(Options options)
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore(options))
            {
                Cluster.SuspendObserver(Server);

                var id = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1)
                }))
                {

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "users/1");
                        session.Store(new User(), "users/2");
                        session.SaveChanges();
                    }

                    var docs = new BlockingCollection<User>();
                    var subscribe = subscription.Run(x => x.Items.ForEach(i => docs.Add(i.Result)));

                    Assert.True(docs.TryTake(out _, _reasonableWaitTime));
                    Assert.True(docs.TryTake(out _, _reasonableWaitTime));
                    await store.Subscriptions.DropConnectionAsync(id);

                    try
                    {
                        // this can exit normally or throw on drop connection
                        // depending on exactly where the drop happens
                        await subscribe.WaitAsync(_reasonableWaitTime);
                    }
                    catch (SubscriptionClosedException)
                    {
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "users/3");
                        session.Store(new User(), "users/4");
                        session.SaveChanges();
                    }


                    Assert.False(docs.TryTake(out var doc, TimeSpan.FromSeconds(0)), doc != null ? doc.ToString() : string.Empty);
                    Assert.False(docs.TryTake(out doc, TimeSpan.FromSeconds(0)), doc != null ? doc.ToString() : string.Empty);

                    Assert.True(subscribe.IsCompleted);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void RavenDB_3453_ShouldDeserializeTheWholeDocumentsAfterTypedSubscription(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(id))
                {
                    var users = new BlockingCollection<User>();

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Age = 31
                        }, "users/1");
                        session.Store(new User
                        {
                            Age = 27
                        }, "users/12");
                        session.Store(new User
                        {
                            Age = 25
                        }, "users/3");

                        session.SaveChanges();
                    }

                    subscription.Run(x => x.Items.ForEach(i => users.Add(i.Result)));

                    var expected = new Dictionary<string, int>
                    {
                        ["users/1"] = 31,
                        ["users/12"] = 27,
                        ["users/3"] = 25
                    };
                    Assert.True(users.TryTake(out User user, _reasonableWaitTime));
                    Assert.True(expected.Remove(user.Id, out var age), $"missing {user.Id}");
                    Assert.Equal(age, user.Age);

                    Assert.True(users.TryTake(out user, _reasonableWaitTime));
                    Assert.True(expected.Remove(user.Id, out age), $"missing {user.Id}");
                    Assert.Equal(age, user.Age);

                    Assert.True(users.TryTake(out user, _reasonableWaitTime));
                    Assert.True(expected.Remove(user.Id, out age), $"missing {user.Id}");
                    Assert.Equal(age, user.Age);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void DisposingOneSubscriptionShouldNotAffectOnNotificationsOfOthers(Options options)
        {
            SubscriptionWorker<User> subscription1 = null;
            SubscriptionWorker<User> subscription2 = null;
            var store = GetDocumentStore(options);
            try
            {
                var id1 = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                var id2 = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());

                using (var s = store.OpenSession())
                {
                    s.Store(new User(), "users/1");
                    s.Store(new User(), "users/2");
                    s.SaveChanges();
                }

                subscription1 = store.Subscriptions.GetSubscriptionWorker<User>(id1);
                var items1 = new BlockingCollection<User>();
                subscription1.Run(x => x.Items.ForEach(i => items1.Add(i.Result)));

                subscription2 = store.Subscriptions.GetSubscriptionWorker<User>(id2);
                var items2 = new BlockingCollection<User>();
                subscription2.Run(x => x.Items.ForEach(i => items2.Add(i.Result)));

                var expected = new List<string> { "users/1", "users/2" };

                Assert.True(items1.TryTake(out var user, _reasonableWaitTime));
                Assert.True(expected.Contains(user.Id), $"missing {user.Id}");
                Assert.True(items1.TryTake(out user, _reasonableWaitTime));
                Assert.True(expected.Contains(user.Id), $"missing {user.Id}");

                Assert.True(items2.TryTake(out user, _reasonableWaitTime));
                Assert.True(expected.Contains(user.Id), $"missing {user.Id}");
                Assert.True(items2.TryTake(out user, _reasonableWaitTime));
                Assert.True(expected.Contains(user.Id), $"missing {user.Id}");


                subscription1.Dispose();

                using (var s = store.OpenSession())
                {
                    s.Store(new User(), "users/3");
                    s.Store(new User(), "users/4");
                    s.SaveChanges();
                }

                expected = new List<string> { "users/3", "users/4" };

                Assert.True(items2.TryTake(out user, _reasonableWaitTime));
                Assert.True(expected.Contains(user.Id), $"missing {user.Id}");
                Assert.True(items2.TryTake(out user, _reasonableWaitTime));
                Assert.True(expected.Contains(user.Id), $"missing {user.Id}");

            }
            finally
            {
                subscription1.Dispose();
                subscription2.Dispose();
                store.Dispose();
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public void AllPropertiesOfSubscriptionCreationOptionsAreInUpdateOfSubscriptionHandler()
        {
            // This test is here to make sure that if we add a new property to SubscriptionCreationOptions
            // we will remember to add it to SubscriptionHasChanges method in SubscriptionHandler as well
            var expected = new List<string>()
            {
                "Name",
                "Query",
                "ChangeVector",
                "MentorNode",
                "Disabled",
                "PinToMentorNode"
            };

            var props = typeof(SubscriptionCreationOptions).GetProperties().Where(x => x.PropertyType.IsPublic).ToList();

            Assert.Equal(expected.Count, props.Count);
            Assert.All(props, x => Assert.Contains(x.Name, expected));
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanUpdateSubscriptionByName(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var subsId = store.Subscriptions.Create(new SubscriptionCreationOptions
                {
                    Query = "from Users",
                    Name = "Created"
                });

                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var state = subscriptions.First();
                Assert.Equal(1, subscriptions.Count);
                Assert.Equal("Created", state.SubscriptionName);
                Assert.Equal("from Users", state.Query);

                var newQuery = "from Users where Age > 18";

                store.Subscriptions.Update(new SubscriptionUpdateOptions
                {
                    Name = subsId,
                    Query = newQuery,
                    Disabled = true
                });

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(newQuery, newState.Query);
                Assert.Equal(state.SubscriptionId, newState.SubscriptionId);
                Assert.True(newState.Disabled);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task CanUpdateSubscriptionPinToMentorNodeByName()
        {
            using (var store = GetDocumentStore())
            {
                var subsId = store.Subscriptions.Create(new SubscriptionCreationOptions
                {
                    Query = "from Users",
                    Name = "Created",
                    MentorNode = "A"
                });

                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var state = subscriptions.First();
                Assert.Equal(1, subscriptions.Count);
                Assert.Equal("Created", state.SubscriptionName);
                Assert.Equal("from Users", state.Query);
                Assert.Equal("A", state.MentorNode);

                store.Subscriptions.Update(new SubscriptionUpdateOptions
                {
                    Name = subsId,
                    PinToMentorNode = true
                });

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(state.SubscriptionId, newState.SubscriptionId);
                Assert.True(newState.PinToMentorNode);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task CanUpdateDisabledByName()
        {
            using (var store = GetDocumentStore())
            {
                var subsId = store.Subscriptions.Create(new SubscriptionCreationOptions
                {
                    Query = "from Users",
                    Name = "Created",
                });

                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var state = subscriptions.First();
                Assert.Equal(1, subscriptions.Count);
                Assert.Equal("Created", state.SubscriptionName);
                Assert.Equal("from Users", state.Query);
                Assert.False(state.Disabled);

                store.Subscriptions.Update(new SubscriptionUpdateOptions
                {
                    Name = subsId,
                    Disabled = true
                });

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(state.SubscriptionId, newState.SubscriptionId);
                Assert.True(newState.Disabled);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(true, null, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, null, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(true, false, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, true, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanUpdateSubscriptionById(Options options, bool create, bool? update)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Subscriptions.Create(new SubscriptionCreationOptions
                {
                    Query = "from Users",
                    Name = "Created",
                    MentorNode = "A",
                    PinToMentorNode = create
                });

                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var state = subscriptions.First();
                Assert.Equal(1, subscriptions.Count);
                Assert.Equal("Created", state.SubscriptionName);
                Assert.Equal("from Users", state.Query);
                Assert.Equal("A", state.MentorNode);
                Assert.Equal(create, state.PinToMentorNode);

                var newQuery = "from Users where Age > 18";

                if (update == null)
                {
                    store.Subscriptions.Update(new SubscriptionUpdateOptions
                    {
                        Query = newQuery,
                        Id = state.SubscriptionId
                    });
                }
                else
                {
                    store.Subscriptions.Update(new SubscriptionUpdateOptions
                    {
                        Query = newQuery,
                        Id = state.SubscriptionId,
                        PinToMentorNode = update.Value,
                        Disabled = update.Value
                    });
                }

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(newQuery, newState.Query);
                Assert.Equal(state.SubscriptionId, newState.SubscriptionId);
                Assert.Equal(state.MentorNode, newState.MentorNode);
                if (update == null)
                {
                    Assert.Equal(state.PinToMentorNode, newState.PinToMentorNode);
                }
                else
                {
                    Assert.Equal(update.Value, newState.PinToMentorNode);
                    Assert.Equal(update.Value, newState.Disabled);
                    Assert.NotEqual(create, newState.PinToMentorNode);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void UpdateNonExistentSubscriptionShouldThrow(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var name = "Update";
                var id = int.MaxValue;
                var idMsg = $"Raven.Client.Exceptions.Documents.Subscriptions.SubscriptionDoesNotExistException: Subscription with id '{id}' was not found in server store";

                var argumentError = Assert.Throws<SubscriptionDoesNotExistException>(() => store.Subscriptions.Update(new SubscriptionUpdateOptions { Name = name }));
                Assert.StartsWith($"Raven.Client.Exceptions.Documents.Subscriptions.SubscriptionDoesNotExistException: Subscription with name '{name}' was not found in server store", argumentError.Message);

                argumentError = Assert.Throws<SubscriptionDoesNotExistException>(() => store.Subscriptions.Update(new SubscriptionUpdateOptions { Name = name, Id = id }));
                Assert.StartsWith(idMsg, argumentError.Message);

                var subsId = store.Subscriptions.Create(new SubscriptionCreationOptions
                {
                    Query = "from Users",
                    Name = "Created"
                });

                argumentError = Assert.Throws<SubscriptionDoesNotExistException>(() => store.Subscriptions.Update(new SubscriptionUpdateOptions { Name = subsId, Id = id }));
                Assert.StartsWith(idMsg, argumentError.Message);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task UpdateSubscriptionShouldReturnNotModified(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var updateOptions = new SubscriptionUpdateOptions
                {
                    Query = "from Users",
                    Name = "Created"
                };
                store.Subscriptions.Create(updateOptions);

                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var state = subscriptions.First();
                Assert.Equal(1, subscriptions.Count);
                Assert.Equal("Created", state.SubscriptionName);
                Assert.Equal("from Users", state.Query);

                store.Subscriptions.Update(updateOptions);

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(state.Query, newState.Query);
                Assert.Equal(state.SubscriptionId, newState.SubscriptionId);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanCreateByUpdateSubscription(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var query = "from Users";
                var name = "Created";
                var id = 1000;

                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(0, subscriptions.Count);

                store.Subscriptions.Update(new SubscriptionUpdateOptions
                {
                    Query = query,
                    Name = name,
                    CreateNew = true
                });

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(1, newSubscriptions.Count);
                var newState = newSubscriptions.First();
                Assert.Equal(name, newState.SubscriptionName);
                Assert.Equal(query, newState.Query);

                store.Subscriptions.Update(new SubscriptionUpdateOptions
                {
                    Query = query,
                    Id = id,
                    CreateNew = true
                });

                newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(2, newSubscriptions.Count);
                newState = newSubscriptions.FirstOrDefault(x => x.SubscriptionName == id.ToString());
                Assert.NotNull(newState);
                Assert.Equal(query, newState.Query);
                Assert.Equal(id, newState.SubscriptionId);

                id++;
                name += "New";
                store.Subscriptions.Update(new SubscriptionUpdateOptions
                {
                    Query = query,
                    Name = name,
                    Id = id,
                    CreateNew = true
                });

                newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(3, newSubscriptions.Count);
                newState = newSubscriptions.FirstOrDefault(x => x.SubscriptionName == name);
                Assert.NotNull(newState);
                Assert.Equal(query, newState.Query);
                Assert.Equal(id, newState.SubscriptionId);

                var oldId = id;
                id++;
                query += " where Age > 322";

                // this should update and not crate a new subscription
                store.Subscriptions.Update(new SubscriptionUpdateOptions
                {
                    Query = query,
                    Name = name,
                    Id = id,
                    CreateNew = true
                });

                newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(3, newSubscriptions.Count);
                newState = newSubscriptions.FirstOrDefault(x => x.SubscriptionName == name);
                Assert.NotNull(newState);
                Assert.Equal(query, newState.Query);
                Assert.Equal(oldId, newState.SubscriptionId);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task CanCreateDisabledSubscriptionByUpdateSubscriptionAndThenUpdate()
        {
            using (var store = GetDocumentStore())
            {
                var query = "from Users";
                var name = "Created";

                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(0, subscriptions.Count);

                store.Subscriptions.Update(new SubscriptionUpdateOptions
                {
                    Query = query,
                    Name = name,
                    Disabled = true,
                    CreateNew = true
                });

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(1, newSubscriptions.Count);
                var newState = newSubscriptions.FirstOrDefault();
                Assert.NotNull(newState);
                Assert.Equal("Created", newState.SubscriptionName);
                Assert.Equal("from Users", newState.Query);
                Assert.True(newState.Disabled);
            }
        }



        class A
        {
            public string Id { get; set; }
            public string BId { get; set; }
        }

        class B
        {
            public string Id { get; set; }
            public string SomeProp { get; set; }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task Subscription_WhenProjectLoad_ShouldTranslateToJavascriptLoad(Options options)
        {
            using var store = GetDocumentStore(options);

            const string someProp = "SomeValue";
            using (var session = store.OpenAsyncSession())
            {
                var b = new B { SomeProp = someProp };
                await session.StoreAsync(b, "b1$a/1");
                await session.StoreAsync(new A { BId = b.Id }, "a/1");
                await session.SaveChangesAsync();
            }

            var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<A>
            {
                Name = "Test subscription",
                Projection = x => new { RavenQuery.Load<B>(x.BId).SomeProp }
            });

            WaitForUserToContinueTheTest(store);

            await using (var sub = store.Subscriptions.GetSubscriptionWorker<ProjectionObject>(name))
            {
                var mre = new AsyncManualResetEvent();
                var subscriptionTask = sub.Run(batch =>
                {
                    Assert.NotEmpty(batch.Items);
                    var projectionObject = batch.Items.First().Result;
                    Assert.Equal("SomeValue", projectionObject.SomeProp);
                    mre.Set();
                });
                var timeout = TimeSpan.FromSeconds(30);
                if (await mre.WaitAsync(timeout) == false)
                {
                    if (subscriptionTask.IsFaulted)
                        await subscriptionTask;

                    throw new TimeoutException($"No batch received for {timeout}");
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Subscription_WhenProjectWithId_ShouldTranslateToJavascriptIdFunction(Options options)
        {
            using var store = GetDocumentStore(options);

            var entity = new User();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>
            {
                Name = "Test subscription",
                Projection = x => new ProjectionObject
                {
                    ProjectionId = x.Id
                }
            });

            await using (var sub = store.Subscriptions.GetSubscriptionWorker<ProjectionObject>(name))
            {
                var mre = new AsyncManualResetEvent();
                var subscriptionTask = sub.Run(batch =>
                {
                    Assert.NotEmpty(batch.Items);
                    string resultOrderId = batch.Items.First().Result.ProjectionId;
                    Assert.Equal(entity.Id, resultOrderId);
                    mre.Set();
                });
                var timeout = TimeSpan.FromSeconds(30);
                if (await mre.WaitAsync(timeout) == false)
                {
                    if (subscriptionTask.IsFaulted)
                        await subscriptionTask;

                    throw new TimeoutException($"No batch received for {timeout}");
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscription_GetOngoingTaskInfoOperation_ShouldReturnCorrentTaskStatus()
        {
            using var store = GetDocumentStore();

            var entity = new User();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

            var state = await store.Subscriptions.GetSubscriptionStateAsync(name);
            await using (var sub = store.Subscriptions.GetSubscriptionWorker<ProjectionObject>(name))
            {
                var mre = new AsyncManualResetEvent();
                var subscriptionTask = sub.Run(batch =>
                {
                    mre.Set();
                });
                var timeout = TimeSpan.FromSeconds(30);
                Assert.True(await mre.WaitAsync(timeout));
                var taskInfoById = store.Maintenance.Send(new GetOngoingTaskInfoOperation(state.SubscriptionId, OngoingTaskType.Subscription));
                Assert.NotNull(taskInfoById);
                Assert.Equal(OngoingTaskState.Enabled, taskInfoById.TaskState);
                Assert.Equal(OngoingTaskType.Subscription, taskInfoById.TaskType);
                Assert.Equal(OngoingTaskConnectionStatus.Active, taskInfoById.TaskConnectionStatus);

                var taskInfoByName = store.Maintenance.Send(new GetOngoingTaskInfoOperation(state.SubscriptionName, OngoingTaskType.Subscription));
                Assert.NotNull(taskInfoByName);
                Assert.Equal(taskInfoById.TaskState, taskInfoByName.TaskState);
                Assert.Equal(taskInfoById.TaskType, taskInfoByName.TaskType);
                Assert.Equal(taskInfoById.TaskConnectionStatus, taskInfoByName.TaskConnectionStatus);
            }
        }

        private class ProjectionObject
        {
            public string SomeProp { get; set; }
            public string ProjectionId { get; set; }
        }

        public class CreateDatabaseOperationWithoutNameValidation : IServerOperation<DatabasePutResult>
        {
            private readonly DatabaseRecord _databaseRecord;
            private readonly int _replicationFactor;

            public CreateDatabaseOperationWithoutNameValidation(DatabaseRecord databaseRecord, int replicationFactor = 1)
            {
                _databaseRecord = databaseRecord;
                _replicationFactor = replicationFactor;
            }

            public RavenCommand<DatabasePutResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new CreateDatabaseOperation.CreateDatabaseCommand(conventions, _databaseRecord, _replicationFactor);
            }
        }
    }
}
