using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Sparrow.Json;
using Sparrow.Platform;
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

        [Fact]
        public void SubscriptionLongName()
        {
            using (var store = GetDocumentStore())
            {
                Assert.Throws<SubscriptionNameException>(() => store.Subscriptions.Create<User>(new SubscriptionCreationOptions<User>
                {
                    Name = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                }));
            }
        }
        [Fact]
        public async Task CanDeleteSubscription()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public async Task ShouldThrowWhenOpeningNoExisingSubscription()
        {
            using (var store = GetDocumentStore())
            {
                var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions("1")
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });
                var ex = await Assert.ThrowsAsync<SubscriptionDoesNotExistException>(() => subscription.Run(x => { }));
            }
        }

        [Fact]
        public async Task ShouldThrowOnAttemptToOpenAlreadyOpenedSubscription()
        {
            using (var store = GetDocumentStore())
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
                        Assert.True(await Assert.ThrowsAsync<SubscriptionInUseException>(() => secondSubscription.Run(x => { })).WaitAsync(_reasonableWaitTime));
                    }
                }
            }
        }

        [Fact]
        public void ShouldBeAbleToChangeBufferSizes()
        {
            using (var store = GetDocumentStore())
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
                    var keys = new BlockingCollection<string>();
                    var ages = new BlockingCollection<int>();

                    subscription.Run(batch =>
                    {
                        batch.Items.ForEach(x => keys.Add(x.Id));
                        batch.Items.ForEach(x => ages.Add(x.Result.Age));
                    });

                    Assert.True(keys.TryTake(out string key, _reasonableWaitTime));
                    Assert.Equal("users/1", key);

                    Assert.True(keys.TryTake(out key, _reasonableWaitTime));
                    Assert.Equal("users/12", key);

                    Assert.True(keys.TryTake(out key, _reasonableWaitTime));
                    Assert.Equal("users/3", key);

                    Assert.True(ages.TryTake(out int age, _reasonableWaitTime));
                    Assert.Equal(31, age);

                    Assert.True(ages.TryTake(out age, _reasonableWaitTime));
                    Assert.Equal(27, age);

                    Assert.True(ages.TryTake(out age, _reasonableWaitTime));
                    Assert.Equal(25, age);

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

        [Fact]
        public void ShouldStreamAllDocumentsAfterSubscriptionCreation()
        {
            using (var store = GetDocumentStore())
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
                    var keys = new BlockingCollection<string>();
                    var ages = new BlockingCollection<int>();

                    subscription.Run(batch =>
                    {
                        batch.Items.ForEach(x => keys.Add(x.Id));
                        batch.Items.ForEach(x => ages.Add(x.Result.Age));
                    });

                    Assert.True(keys.TryTake(out string key, _reasonableWaitTime));
                    Assert.Equal("users/1", key);

                    Assert.True(keys.TryTake(out key, _reasonableWaitTime));
                    Assert.Equal("users/12", key);

                    Assert.True(keys.TryTake(out key, _reasonableWaitTime));
                    Assert.Equal("users/3", key);

                    Assert.True(ages.TryTake(out int age, _reasonableWaitTime));
                    Assert.Equal(31, age);

                    Assert.True(ages.TryTake(out age, _reasonableWaitTime));
                    Assert.Equal(27, age);

                    Assert.True(ages.TryTake(out age, _reasonableWaitTime));
                    Assert.Equal(25, age);

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

        [Fact]
        public void ShouldSendAllNewAndModifiedDocs()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public void ShouldRespectMaxDocCountInBatch()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public void ShouldRespectCollectionCriteria()
        {
            using (var store = GetDocumentStore())
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

        [Fact(Skip = "RavenDB-8404, RavenDB-8682")]
        public void ShouldRespectStartsWithCriteria()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public void WillAcknowledgeEmptyBatches()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public async Task ShouldKeepPullingDocsAfterServerRestart()
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

                store = new DocumentStore()
                {
                    Urls = new[] { server.ServerStore.GetNodeHttpServerUrl() },
                    Database = "RavenDB_2627",

                }.Initialize();

                var doc = new DatabaseRecord(store.Database);
                var result = store.Maintenance.Server.Send(new CreateDatabaseOperationWithoutNameValidation(doc));
                await WaitForRaftIndexToBeAppliedInCluster(result.RaftCommandIndex, _reasonableWaitTime);

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


                var gotBatch = new ManualResetEventSlim();
                var gotArek = new ManualResetEventSlim();
                var t = subscriptionWorker.Run(x =>
                {
                    gotBatch.Set();

                    foreach (var item in x.Items)
                    {
                        if (item.Id == "users/arek")
                            gotArek.Set();
                    }
                });

                Assert.True(gotBatch.Wait(_reasonableWaitTime));

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

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

                Assert.True(gotArek.Wait(_reasonableWaitTime));
            }
            finally
            {
                subscriptionWorker?.Dispose();
                store?.Dispose();
                server.Dispose();
            }
        }

        [Fact]
        public async Task CanReleaseSubscription()
        {
            SubscriptionWorker<dynamic> subscriptionWorker = null;
            SubscriptionWorker<dynamic> throwingSubscriptionWorker = null;
            SubscriptionWorker<dynamic> notThrowingSubscriptionWorker = null;

            var store = GetDocumentStore();
            try
            {
                Server.ServerStore.Observer.Suspended = true;
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
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

                Assert.True(await Assert.ThrowsAsync<SubscriptionInUseException>(() =>
                {
                    return subscriptionTask;
                }).WaitAsync(_reasonableWaitTime));

                store.Subscriptions.DropConnection(id);

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

        [Fact]
        public void ShouldPullDocumentsAfterBulkInsert()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public async Task ShouldStopPullingDocsAndCloseSubscriptionOnSubscriberErrorByDefault()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public void CanSetToIgnoreSubscriberErrors()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public async Task RavenDB_3452_ShouldStopPullingDocsIfReleased()
        {
            using (var store = GetDocumentStore())
            {
                Server.ServerStore.Observer.Suspended = true;
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());

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
                    store.Subscriptions.DropConnection(id);

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

        [Fact]
        public void RavenDB_3453_ShouldDeserializeTheWholeDocumentsAfterTypedSubscription()
        {
            using (var store = GetDocumentStore())
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

                    User user;
                    Assert.True(users.TryTake(out user, _reasonableWaitTime));
                    Assert.Equal("users/1", user.Id);
                    Assert.Equal(31, user.Age);

                    Assert.True(users.TryTake(out user, _reasonableWaitTime));
                    Assert.Equal("users/12", user.Id);
                    Assert.Equal(27, user.Age);

                    Assert.True(users.TryTake(out user, _reasonableWaitTime));
                    Assert.Equal("users/3", user.Id);
                    Assert.Equal(25, user.Age);
                }
            }
        }

        [Fact]
        public void DisposingOneSubscriptionShouldNotAffectOnNotificationsOfOthers()
        {
            SubscriptionWorker<User> subscription1 = null;
            SubscriptionWorker<User> subscription2 = null;
            var store = GetDocumentStore();
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


                Assert.True(items1.TryTake(out var user, _reasonableWaitTime));
                Assert.Equal("users/1", user.Id);
                Assert.True(items1.TryTake(out user, _reasonableWaitTime));
                Assert.Equal("users/2", user.Id);

                Assert.True(items2.TryTake(out user, _reasonableWaitTime));
                Assert.Equal("users/1", user.Id);
                Assert.True(items2.TryTake(out user, _reasonableWaitTime));
                Assert.Equal("users/2", user.Id);

                subscription1.Dispose();

                using (var s = store.OpenSession())
                {
                    s.Store(new User(), "users/3");
                    s.Store(new User(), "users/4");
                    s.SaveChanges();
                }

                Assert.True(items2.TryTake(out user, _reasonableWaitTime));
                Assert.Equal("users/3", user.Id);
                Assert.True(items2.TryTake(out user, _reasonableWaitTime));
                Assert.Equal("users/4", user.Id);
            }
            finally
            {
                subscription1.Dispose();
                subscription2.Dispose();
                store.Dispose();
            }
        }

        [Fact]
        public async Task CanUpdateSubscriptionByName()
        {
            using (var store = GetDocumentStore())
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
                    Query = newQuery
                });

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(newQuery, newState.Query);
                Assert.Equal(state.SubscriptionId, newState.SubscriptionId);
            }
        }

        [Fact]
        public async Task CanUpdateSubscriptionById()
        {
            using (var store = GetDocumentStore())
            {
                store.Subscriptions.Create(new SubscriptionCreationOptions
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
                    Query = newQuery,
                    Id = state.SubscriptionId
                });

                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(state.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(newQuery, newState.Query);
                Assert.Equal(state.SubscriptionId, newState.SubscriptionId);
            }
        }

        [Fact]
        public void UpdateNonExistentSubscriptionShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                var name = "Update";
                var id = 322;
                var idMsg = $"Raven.Client.Exceptions.Documents.Subscriptions.SubscriptionDoesNotExistException: Subscription with id '{id}' was not found in server store";

                var argumentError = Assert.Throws<SubscriptionDoesNotExistException>(() => store.Subscriptions.Update(new SubscriptionUpdateOptions { Name = name }));
                Assert.StartsWith($"Raven.Client.Exceptions.Documents.Subscriptions.SubscriptionDoesNotExistException: Subscription with name '{name}' was not found in server store", argumentError.Message);
                
                argumentError = Assert.Throws<SubscriptionDoesNotExistException>(() => store.Subscriptions.Update(new SubscriptionUpdateOptions { Name = name, Id = id }) );
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

        [Fact]
        public async Task UpdateSubscriptionShouldReturnNotModified()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public async Task CanCreateByUpdateSubscription()
        {
            using (var store = GetDocumentStore())
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
                newState = newSubscriptions.FirstOrDefault(x=>x.SubscriptionName == id.ToString());
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
                return new CreateDatabaseOperation.CreateDatabaseCommand(_databaseRecord, _replicationFactor);
            }
        }
    }
}
