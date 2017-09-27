using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Extensions;

namespace FastTests.Client.Subscriptions
{
    public class SubscriptionsBasic : RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

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
                var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions("1"));
                var ex = await Assert.ThrowsAsync<SubscriptionDoesNotExistException>(() => subscription.Run(x => { }));
            }
        }

        [Fact]
        public async Task ShouldThrowOnAttemptToOpenAlreadyOpenedSubscription()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                using (var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User());
                        session.SaveChanges();
                    }

                    var amre = new AsyncManualResetEvent();
                    subscription.Run(x => amre.Set());

                    await amre.WaitAsync(_reasonableWaitTime);

                    using (var secondSubscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = SubscriptionOpeningStrategy.OpenIfFree
                    }))
                    {
                        Assert.True(await Assert.ThrowsAsync<SubscriptionInUseException>(() => secondSubscription.Run(x => { })).WaitAsync(_reasonableWaitTime));
                    }
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
                using (var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)))
                {

                    var keys = new BlockingCollection<string>();
                    var ages = new BlockingCollection<int>();

                    subscription.Run(batch =>
                    {
                        batch.Items.ForEach(x => keys.Add(x.Id));
                        batch.Items.ForEach(x => ages.Add(x.Result.Age));
                    });

                    string key;
                    Assert.True(keys.TryTake(out key, _reasonableWaitTime));
                    Assert.Equal("users/1", key);

                    Assert.True(keys.TryTake(out key, _reasonableWaitTime));
                    Assert.Equal("users/12", key);

                    Assert.True(keys.TryTake(out key, _reasonableWaitTime));
                    Assert.Equal("users/3", key);

                    int age;
                    Assert.True(ages.TryTake(out age, _reasonableWaitTime));
                    Assert.Equal(31, age);

                    Assert.True(ages.TryTake(out age, _reasonableWaitTime));
                    Assert.Equal(27, age);

                    Assert.True(ages.TryTake(out age, _reasonableWaitTime));
                    Assert.Equal(25, age);
                }
            }
        }

        [Fact]
        public void ShouldSendAllNewAndModifiedDocs()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                using (var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)))
                {
                    var names = new BlockingCollection<string>();

                    subscription.Run(batch => batch.Items.ForEach(x => names.Add(x.Result.Name)));

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "James"
                        }, "users/1");
                        session.SaveChanges();
                    }

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
                using (var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                    {
                        MaxDocsPerBatch = 25
                    }
                ))
                {

                    var batchSizes = new ConcurrentStack<Reference<int>>();

                    batchSizes.Push(new Reference<int>());

                    subscription.AfterAcknowledgment += x =>
                    {
                        batchSizes.Push(new Reference<int>());
                        return Task.CompletedTask;
                    };

                    subscription.Run(batch =>
                    {
                        Reference<int> reference;
                        if (batchSizes.TryPeek(out reference))
                            reference.Value += batch.Items.Count;
                    });

                    var result = SpinWait.SpinUntil(() => batchSizes.ToList().Sum(x => x.Value) >= 100, TimeSpan.FromSeconds(60));

                    Assert.True(result);

                    Assert.Equal(4, batchSizes.Count);

                    foreach (var reference in batchSizes)
                    {
                        Assert.Equal(25, reference.Value);
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

                using (var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    MaxDocsPerBatch = 31
                }))
                {
                    var docs = new ConcurrentBag<User>();
                    subscription.Run(batch => batch.Items.ForEach(x => docs.Add(x.Result)));
                    Assert.True(SpinWait.SpinUntil(() => docs.Count >= 100, TimeSpan.FromSeconds(60)));
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

                using (var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    MaxDocsPerBatch = 15
                }))
                { 

                    var docIDs = new ConcurrentBag<string>();

                subscription.Run(x=>x.Items.ForEach(i=> docIDs.Add(i.Id)));

                Assert.True(SpinWait.SpinUntil(() => docIDs.Count >= 50, TimeSpan.FromSeconds(60)));


                foreach (var docId in docIDs)
                {
                    Assert.True(docId.StartsWith("users/favorite/"));
                }
                }
            }
        }
              
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
                Assert.Equal("from Users", subscriptionDocuments[0].Query);

                var subscription = store.Subscriptions.Open(                     
                    new SubscriptionConnectionOptions(subscriptionDocuments[0].SubscriptionName));

                var docs = new ConcurrentBag<User>();
                subscription.Run(x=>x.Items.ForEach(i=>docs.Add(i.Result)));

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                Assert.True(SpinWait.SpinUntil(() => docs.Count >= 1, TimeSpan.FromSeconds(10)));
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
                using (var allSubscription = store.Subscriptions.Open(allId))
                {
                    var allDocs = new ConcurrentBag<User>();
                    allSubscription.Run(x => x.Items.ForEach(i => allDocs.Add(i.Result)));

                    var filteredUsersId = store.Subscriptions.Create(new SubscriptionCreationOptions
                    {
                        Query=@"from Users where Age <0"
                    });
                    using (var filteredUsersSubscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(filteredUsersId)))
                    {
                        var usersDocs = new ConcurrentBag<User>();
                        filteredUsersSubscription.Run(x => x.Items.ForEach(i => usersDocs.Add(i.Result)));

                        using (var session = store.OpenSession())
                        {
                            for (int i = 0; i < 500; i++)
                                session.Store(new User(), "another/");
                            session.SaveChanges();
                        }

                        Assert.True(SpinWait.SpinUntil(() => allDocs.Count == 500, TimeSpan.FromSeconds(10)));
                        Assert.True(SpinWait.SpinUntil(() =>
                        {
                            subscriptionDocuments = store.Subscriptions.GetSubscriptions(0, 10);

                            return subscriptionDocuments[0].ChangeVectorForNextBatchStartingPoint == subscriptionDocuments[1].ChangeVectorForNextBatchStartingPoint;
                        }, TimeSpan.FromSeconds(10)));

                        Assert.Equal(500, allDocs.Count);
                        Assert.Equal(0, usersDocs.Count);
                    }
                }
            }
        }

        [Fact]
        public async Task ShouldKeepPullingDocsAfterServerRestart()
        {
            var dataPath = NewDataPath("RavenDB_2627_after_restart");

            IDocumentStore store = null;
            RavenServer server = null;
            Subscription<dynamic> subscription = null;
            try
            {
                var serverDisposed = new ManualResetEventSlim(false);

                server = GetNewServer(runInMemory: false, customSettings: new Dictionary<string, string>()
                {
                    [RavenConfiguration.GetKey(x=>x.Core.DataDirectory)] = dataPath
                });
               
                store = new DocumentStore()
                {
                    Urls = new[] { server.ServerStore.NodeHttpServerUrl },
                    Database = "RavenDB_2627",

                }.Initialize();
                
                var doc = new DatabaseRecord(store.Database);
                var result = store.Admin.Server.Send(new CreateDatabaseOperationWithoutNameValidation(doc));
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

                subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1),
                    MaxDocsPerBatch = 1                    
                });
                

                var userIDs = new BlockingCollection<string>();

                var shouldDisposeServer = true;
                subscription.Run(x=> {
                    x.Items.ForEach(i => userIDs.Add(i.Id));
                    if (shouldDisposeServer)
                    {
                        Server.ServerStore.DatabasesLandlord.UnloadDatabase(store.Database);
                    }
                });               

                serverDisposed.Wait(_reasonableWaitTime);
                shouldDisposeServer = false;
                
                Assert.True(SpinWait.SpinUntil(() =>
                {
                    var wasAbleToStore = true;
                    try
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(new User(), "users/arek");
                            session.SaveChanges();
                        }
                    }
                    catch
                    {
                        wasAbleToStore = false;
                    }

                    return wasAbleToStore;
                }, _reasonableWaitTime));

                for (int i = 0; i < 10; i++)
                {
                    string docId;
                    Assert.True(userIDs.TryTake(out docId, _reasonableWaitTime));
                    if ("users/arek" == docId)
                        return;
                }
                Assert.False(true, "didn't get the username");

            }
            finally
            {
                subscription?.Dispose();
                store?.Dispose();
                server.Dispose();
            }
        }      
        
        [Fact]
        public async Task CanReleaseSubscription()
        {
            Subscription<dynamic> subscription = null;
            Subscription<dynamic> throwingSubscription = null;
            Subscription<dynamic> notThrowingSubscription = null;

            var store = GetDocumentStore();
            try
            {
                Server.ServerStore.Observer.Suspended = true;
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.OpenIfFree
                });
                var mre = new AsyncManualResetEvent();
                PutUserDoc(store);
#pragma warning disable 4014
                subscription.Run(x =>
#pragma warning restore 4014
                {
                    mre.Set();
                });
                Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                mre.Reset();

                throwingSubscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.OpenIfFree
                });
                var subscriptionTask = throwingSubscription.Run(x => { });
                
                Assert.True(await Assert.ThrowsAsync<SubscriptionInUseException>(() =>
                {
                    return subscriptionTask;
                }).WaitAsync(_reasonableWaitTime));

                store.Subscriptions.DropConnection(id);

                notThrowingSubscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id));

#pragma warning disable 4014
                notThrowingSubscription.Run(x =>
#pragma warning restore 4014
                {
                    mre.Set();
                });

                PutUserDoc(store);

                Assert.True(await mre.WaitAsync(_reasonableWaitTime));
            }
            finally
            {
                subscription?.Dispose();
                throwingSubscription?.Dispose();
                notThrowingSubscription?.Dispose();
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
                using (var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)))
                {

                    var docs = new BlockingCollection<User>();
                    subscription.Run(x => x.Items.ForEach(i => docs.Add(i.Result)));

                    using (var bulk = store.BulkInsert())
                    {
                        bulk.Store(new User());
                        bulk.Store(new User());
                        bulk.Store(new User());
                    }

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
                using (var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)))
                {
                    var subscriptionTask = subscription.Run(x => throw new Exception("Fake exception"));
                    PutUserDoc(store);
                    
                    await subscriptionTask.WaitAsync(_reasonableWaitTime);
                    Assert.True(await Assert.ThrowsAsync<SubscriberErrorException>(() => subscriptionTask).WaitAsync(_reasonableWaitTime));
                    
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
                using (var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    IgnoreSubscriberErrors = true
                }))
                {

                    var docs = new BlockingCollection<User>();
                    var subscriptionTask = subscription.Run(x =>
                    {
                        x.Items.ForEach(i => docs.Add(i.Result));
                        throw new Exception("Fake exception");
                    });

                    PutUserDoc(store);
                    PutUserDoc(store);

                    Assert.True(docs.TryTake(out _, _reasonableWaitTime));
                    Assert.True(docs.TryTake(out _, _reasonableWaitTime));
                    Assert.Null(subscriptionTask.Exception);
                }
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
                    Filter = x=>x.Address.Street == "1st Street" && x.Address.ZipCode != 999
                });

                using (var carolines = store.Subscriptions.Open<PersonWithAddress>(new SubscriptionConnectionOptions(id)
                {
                    MaxDocsPerBatch = 5
                }))
                {
                    var docs = new BlockingCollection<PersonWithAddress>();
                    carolines.Run(x => x.Items.ForEach(i => docs.Add(i.Result)));

                    Assert.True(SpinWait.SpinUntil(() => docs.Count >= 5, TimeSpan.FromSeconds(60)));

                    foreach (var user in docs)
                    {
                        Assert.Equal("1st Street", user.Address.Street);
                    }
                }
            }
        }

        [Fact]
        public void RavenDB_3452_ShouldStopPullingDocsIfReleased()
        {
            using (var store = GetDocumentStore())
            {
                Server.ServerStore.Observer.Suspended = true;
                var id = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());

                using (var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
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
                    
                    Assert.True(SpinWait.SpinUntil(()=>subscribe.IsCompleted,_reasonableWaitTime));

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "users/3");
                        session.Store(new User(), "users/4");
                        session.SaveChanges();
                    }


                    Assert.False(docs.TryTake(out var doc, TimeSpan.FromSeconds(6)), doc != null ? doc.ToString() : string.Empty);
                    Assert.False(docs.TryTake(out doc, TimeSpan.FromSeconds(6)), doc != null ? doc.ToString() : string.Empty);

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
                using (var subscription = store.Subscriptions.Open<User>(id))
                {

                    var users = new BlockingCollection<User>();

                    subscription.Run(x => x.Items.ForEach(i => users.Add(i.Result)));

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
            Subscription<User> subscription1 = null;
            Subscription<User> subscription2 = null;
            var store = GetDocumentStore();
            try
            {
                var id1 = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                var id2 = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());

                subscription1 = store.Subscriptions.Open<User>(id1);
                var items1 = new BlockingCollection<User>();
                subscription1.Run(x => x.Items.ForEach(i => items1.Add(i.Result)));

                subscription2 = store.Subscriptions.Open<User>(id2);
                var items2 = new BlockingCollection<User>();
                subscription2.Run(x => x.Items.ForEach(i => items2.Add(i.Result)));

                using (var s = store.OpenSession())
                {
                    s.Store(new User(), "users/1");
                    s.Store(new User(), "users/2");
                    s.SaveChanges();
                }

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
        public async Task RunningSubscriptionShouldJumpToNextChangeVectorIfItWasChangedByAdmin()
        {
            using (var store = GetDocumentStore())
            {
                Server.ServerStore.Observer.Suspended = true;
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                using (var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                {
                    MaxDocsPerBatch = 1
                }))
                {
                    var users = new BlockingCollection<User>();
                    string cvFirst = null;
                    string cvBigger = null;
                    var database = await GetDatabase(store.Database);

                    subscription.Run(x => x.Items.ForEach(i => users.Add(i.Result)));

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

                    var firstItemchangeVector = cvFirst.ToChangeVector();
                    firstItemchangeVector[0].Etag = 20;
                    cvBigger = firstItemchangeVector.SerializeVector();

                    SubscriptionState subscriptionState = null;
                    Assert.True(SpinWait.SpinUntil(() =>
                    {
                        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            subscriptionState = database.SubscriptionStorage.GetSubscriptionFromServerStore(context, subscriptionId);
                            var updatedChangeVector = subscriptionState.ChangeVectorForNextBatchStartingPoint;
                            // ReSharper disable once StringCompareToIsCultureSpecific
                            
                            return updatedChangeVector != null && cvFirst.CompareTo(updatedChangeVector) == 0;
                        }
                    }, _reasonableWaitTime));


                    await database.SubscriptionStorage.PutSubscription(new SubscriptionCreationOptions()
                    {
                        ChangeVector = cvBigger,
                        Name = subscriptionState.SubscriptionName,
                        Query =  subscriptionState.Query
                    }, subscriptionState.SubscriptionId, false);

                    Assert.True(SpinWait.SpinUntil(() =>
                    {
                        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            subscriptionState = database.SubscriptionStorage.GetSubscriptionFromServerStore(context, subscriptionId);
                            var updatedEtag = subscriptionState.ChangeVectorForNextBatchStartingPoint;
                            return updatedEtag != null && cvBigger.CompareTo(updatedEtag) == 0;
                        }
                    }, _reasonableWaitTime));

                    using (var session = store.OpenSession())
                    {
                        for (var i = 0; i < 20; i++)
                        {
                            session.Store(new User
                            {
                                Name = "Adam",
                                Age = 21 +i
                            }, "users/");
                        }
                        session.SaveChanges();
                    }
                    var usersSkipped = 0;
                    
                    Assert.True(SpinWait.SpinUntil(() =>
                    {
                        if (users.TryTake(out var user, TimeSpan.FromSeconds(1)))
                        {
                            usersSkipped++;
                            return user?.Age == 40;
                        }
                        return false;

                    }, _reasonableWaitTime));

                    Assert.Equal(usersSkipped,2);

                    string afterAnotherUpdateEtag = null;
                    using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        subscriptionState = database.SubscriptionStorage.GetSubscriptionFromServerStore(context, subscriptionId);
                        afterAnotherUpdateEtag = subscriptionState.ChangeVectorForNextBatchStartingPoint;
                    }
                    Assert.True(cvBigger.CompareTo(afterAnotherUpdateEtag) == 0);
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

                var subscription = store.Subscriptions.Open<Company>(new SubscriptionConnectionOptions(id)
                {
                    MaxDocsPerBatch = 1,
                    IgnoreSubscriberErrors = true
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
                return new CreateDatabaseCommand(conventions, context, _databaseRecord, this);
            }

            private class CreateDatabaseCommand : RavenCommand<DatabasePutResult>
            {
                private readonly JsonOperationContext _context;
                private readonly CreateDatabaseOperationWithoutNameValidation _createDatabaseOperation;
                private readonly BlittableJsonReaderObject _databaseDocument;
                private readonly string _databaseName;

                public CreateDatabaseCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseRecord databaseRecord,
                    CreateDatabaseOperationWithoutNameValidation createDatabaseOperation)
                {
                    if (conventions == null)
                        throw new ArgumentNullException(nameof(conventions));

                    _context = context ?? throw new ArgumentNullException(nameof(context));
                    _createDatabaseOperation = createDatabaseOperation;
                    _databaseName = databaseRecord?.DatabaseName ?? throw new ArgumentNullException(nameof(databaseRecord));
                    _databaseDocument = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, conventions, context);
                }
                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/admin/databases?name={_databaseName}";

                    url += "&replication-factor=" + _createDatabaseOperation._replicationFactor;

                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Put,
                        Content = new BlittableJsonContent(stream =>
                        {
                            _context.Write(stream, _databaseDocument);
                        })
                    };

                    return request;
                }

                public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        ThrowInvalidResponse();

                    Result = JsonDeserializationClient.DatabasePutResult(response);
                }

                public override bool IsReadRequest => false;
            }
        }

    }
   
}
