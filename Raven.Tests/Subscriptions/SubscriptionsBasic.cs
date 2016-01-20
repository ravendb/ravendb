// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2627.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Actions;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Subscriptions
{
    public class SubscriptionsBasic : RavenTest
    {
        private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);

        [Fact]
        public void CanCreateSubscription()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                Assert.Equal(1, id);

                id = store.Subscriptions.Create(new SubscriptionCriteria());
                Assert.Equal(2, id);
            }
        }

        [Fact]
        public void CanDeleteSubscription()
        {
            using (var store = NewDocumentStore())
            {
                var id1 = store.Subscriptions.Create(new SubscriptionCriteria());
                var id2 = store.Subscriptions.Create(new SubscriptionCriteria());

                var subscriptions = store.Subscriptions.GetSubscriptions(0, 5);

                Assert.Equal(2, subscriptions.Count);

                store.Subscriptions.Delete(id1);
                store.Subscriptions.Delete(id2);

                subscriptions = store.Subscriptions.GetSubscriptions(0, 5);

                Assert.Equal(0, subscriptions.Count);
            }
        }

        [Fact]
        public void ShouldThrowWhenOpeningNoExisingSubscription()
        {
            using (var store = NewDocumentStore())
            {
                var ex = Assert.Throws<SubscriptionDoesNotExistException>(() => store.Subscriptions.Open(1, new SubscriptionConnectionOptions()));
                Assert.Equal("There is no subscription configuration for specified identifier (id: 1)", ex.Message);
            }
        }

        [Fact]
        public void ShouldThrowOnAttemptToOpenAlreadyOpenedSubscription()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                store.Subscriptions.Open(id, new SubscriptionConnectionOptions());

                var ex = Assert.Throws<SubscriptionInUseException>(() => store.Subscriptions.Open(id, new SubscriptionConnectionOptions()));
                Assert.Equal("Subscription is already in use. There can be only a single open subscription connection per subscription.", ex.Message);
            }
        }

        [Fact]
        public void ShouldStreamAllDocumentsAfterSubscriptionCreation()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Age = 31}, "users/1");
                    session.Store(new User { Age = 27}, "users/12");
                    session.Store(new User { Age = 25}, "users/3");

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions());

                var keys = new BlockingCollection<string>();
                var ages = new BlockingCollection<int>();

                subscription.Subscribe(x => keys.Add(x[Constants.Metadata].Value<string>("@id")));
                subscription.Subscribe(x => ages.Add(x.Value<int>("Age")));

                string key;
                Assert.True(keys.TryTake(out key, waitForDocTimeout));
                Assert.Equal("users/1", key);

                Assert.True(keys.TryTake(out key, waitForDocTimeout));
                Assert.Equal("users/12", key);

                Assert.True(keys.TryTake(out key, waitForDocTimeout));
                Assert.Equal("users/3", key);

                int age;
                Assert.True(ages.TryTake(out age, waitForDocTimeout));
                Assert.Equal(31, age);

                Assert.True(ages.TryTake(out age, waitForDocTimeout));
                Assert.Equal(27, age);

                Assert.True(ages.TryTake(out age, waitForDocTimeout));
                Assert.Equal(25, age);
            }
        }

        [Fact]
        public void ShouldSendAllNewAndModifiedDocs()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions());

                var names = new BlockingCollection<string>();
                store.Changes().WaitForAllPendingSubscriptions();

                subscription.Subscribe(x => names.Add(x.Value<string>("Name")));
                
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "James" }, "users/1");
                    session.SaveChanges();
                }

                string name;
                Assert.True(names.TryTake(out name, waitForDocTimeout));
                Assert.Equal("James", name);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Adam"}, "users/12");
                    session.SaveChanges();
                }

                Assert.True(names.TryTake(out name, waitForDocTimeout));
                Assert.Equal("Adam", name);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "David"}, "users/1");
                    session.SaveChanges();
                }

                Assert.True(names.TryTake(out name, waitForDocTimeout));
                Assert.Equal("David", name);
            }
        }

        [Fact]
        public void ShouldResendDocsIfAcknowledgmentTimeoutOccurred()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                var subscriptionZeroTimeout = store.Subscriptions.Open(id, new SubscriptionConnectionOptions
                {
                    BatchOptions = new SubscriptionBatchOptions()
                    {
                        AcknowledgmentTimeout = TimeSpan.FromMilliseconds(-10) // the client won't be able to acknowledge in negative time
                    }
                });
                store.Changes().WaitForAllPendingSubscriptions();
                var docs = new BlockingCollection<RavenJObject>();

                subscriptionZeroTimeout.Subscribe(docs.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Raven"});
                    session.SaveChanges();
                }

                RavenJObject document;

                Assert.True(docs.TryTake(out document, waitForDocTimeout));
                Assert.Equal("Raven", document.Value<string>("Name"));

                Assert.True(docs.TryTake(out document, waitForDocTimeout));
                Assert.Equal("Raven", document.Value<string>("Name"));

                Assert.True(docs.TryTake(out document, waitForDocTimeout));
                Assert.Equal("Raven", document.Value<string>("Name"));

                subscriptionZeroTimeout.Dispose();

                // retry with longer timeout - should sent just one doc

                var subscriptionLongerTimeout = store.Subscriptions.Open(id, new SubscriptionConnectionOptions
                {
                    BatchOptions = new SubscriptionBatchOptions()
                    {
                        AcknowledgmentTimeout = TimeSpan.FromSeconds(30)
                    }
                });

                var docs2 = new BlockingCollection<RavenJObject>();

                subscriptionLongerTimeout.Subscribe(docs2.Add);

                Assert.True(docs2.TryTake(out document, waitForDocTimeout));
                Assert.Equal("Raven", document.Value<string>("Name"));

                Assert.False(docs2.TryTake(out document, waitForDocTimeout));
            }
        }


        [Fact]
        public void ShouldRespectMaxDocCountInBatch()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions{ BatchOptions = new SubscriptionBatchOptions { MaxDocCount = 25 }});

                var batchSizes = new ConcurrentStack<Reference<int>>();

                subscription.BeforeBatch +=
                    () => batchSizes.Push(new Reference<int>());

                subscription.Subscribe(x =>
                {
                    Reference<int> reference;
                    batchSizes.TryPeek(out reference);
                    reference.Value++;
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

        [Fact]
        public void ShouldRespectMaxBatchSize()
        {
            using (var store = NewDocumentStore())
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

                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions()
                {
                    BatchOptions = new SubscriptionBatchOptions()
                    {
                        MaxSize = 16 * 1024
                    }
                });

                var batches = new ConcurrentStack<ConcurrentBag<RavenJObject>>();

                subscription.BeforeBatch += () => batches.Push(new ConcurrentBag<RavenJObject>());

                subscription.Subscribe(x =>
                {
                    ConcurrentBag<RavenJObject> list;
                    batches.TryPeek(out list);
                    list.Add(x);
                });

                var result = SpinWait.SpinUntil(() => batches.ToList().Sum(x => x.Count) >= 200, TimeSpan.FromSeconds(10));

                Assert.True(result);
                Assert.True(batches.Count > 1);
            }
        }

        [Fact]
        public void ShouldRespectCollectionCriteria()
        {
            using (var store = NewDocumentStore())
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

                var id = store.Subscriptions.Create(new SubscriptionCriteria
                {
                    BelongsToAnyCollection = new [] { "Users" }
                });

                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions
                {
                    BatchOptions = new SubscriptionBatchOptions { MaxDocCount = 31 }
                });

                var docs = new ConcurrentBag<RavenJObject>();

                subscription.Subscribe(docs.Add);

                Assert.True(SpinWait.SpinUntil(() => docs.Count >= 100, TimeSpan.FromSeconds(60)));


                foreach (var jsonDocument in docs)
                {
                    Assert.Equal("Users", jsonDocument[Constants.Metadata].Value<string>(Constants.RavenEntityName));
                }
            }
        }

        [Fact]
        public void ShouldRespectStartsWithCriteria()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.Store(new User(), i % 2 == 0 ? "users/" : "users/favorite/");
                    }

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria
                {
                    KeyStartsWith = "users/favorite/"
                });

                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions
                {
                    BatchOptions = new SubscriptionBatchOptions()
                    {
                        MaxDocCount = 15
                    }
                });

                var docs = new ConcurrentBag<RavenJObject>();

                subscription.Subscribe(docs.Add);

                Assert.True(SpinWait.SpinUntil(() => docs.Count >= 50, TimeSpan.FromSeconds(60)));


                foreach (var jsonDocument in docs)
                {
                    Assert.True(jsonDocument[Constants.Metadata].Value<string>("@id").StartsWith("users/favorite/"));
                }
            }
        }

        [Fact]
        public void ShouldRespectPropertiesCriteria()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Name = i % 2 == 0 ? "Jessica" : "Caroline"
                        });

                        session.Store(new Person
                        {
                            Name = i % 2 == 0 ? "Caroline" : "Samantha"
                        });

                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria
                {
                    PropertiesMatch = new Dictionary<string, RavenJToken>()
                    {
                        {"Name", "Caroline"}
                    }
                });

                var carolines = store.Subscriptions.Open(id, new SubscriptionConnectionOptions { BatchOptions = new SubscriptionBatchOptions { MaxDocCount = 5 }});

                var docs = new ConcurrentBag<RavenJObject>();

                carolines.Subscribe(docs.Add);

                Assert.True(SpinWait.SpinUntil(() => docs.Count >= 10, TimeSpan.FromSeconds(60)));


                foreach (var jsonDocument in docs)
                {
                    Assert.Equal("Caroline", jsonDocument.Value<string>("Name"));
                }
            }
        }

        [Fact]
        public void ShouldRespectPropertiesNotMatchCriteria()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Name = i % 2 == 0 ? "Jessica" : "Caroline"
                        });

                        session.Store(new Person
                        {
                            Name = i % 2 == 0 ? "Caroline" : "Samantha"
                        });

                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria
                {
                    PropertiesNotMatch = new Dictionary<string, RavenJToken>()
                    {
                        {"Name", "Caroline"}
                    }
                });

                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions
                {
                    BatchOptions = new SubscriptionBatchOptions()
                    {
                        MaxDocCount = 5
                    }
                });

                var docs = new ConcurrentBag<RavenJObject>();

                subscription.Subscribe(docs.Add);

                Assert.True(SpinWait.SpinUntil(() => docs.Count >= 20, TimeSpan.FromSeconds(60)));


                foreach (var jsonDocument in docs)
                {
                    Assert.True(jsonDocument.ContainsKey("Name") == false || jsonDocument.Value<string>("Name") != "Caroline");
                }
            }
        }

        [Fact]
        public void CanGetSubscriptionsFromDatabase()
        {
            using (var store = NewDocumentStore())
            {
                var subscriptionDocuments = store.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(0, subscriptionDocuments.Count);

                store.Subscriptions.Create(new SubscriptionCriteria
                {
                    KeyStartsWith = "users/"
                });

                subscriptionDocuments = store.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(1, subscriptionDocuments.Count);
                Assert.Equal("users/", subscriptionDocuments[0].Criteria.KeyStartsWith);

                var subscription = store.Subscriptions.Open(subscriptionDocuments[0].SubscriptionId, new SubscriptionConnectionOptions());

                var docs = new ConcurrentBag<RavenJObject>();
                subscription.Subscribe(docs.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                Assert.True(SpinWait.SpinUntil(() => docs.Count >= 1, TimeSpan.FromSeconds(10)));
            }
        }

        [Theory]
        [PropertyData("Storages")]

        public void CanFilterSubscriptionsWithSpecificPrefixes(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage:storage))
            {
                var subscriptionDocuments = store.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(0, subscriptionDocuments.Count);

                long allId = store.Subscriptions.Create(new SubscriptionCriteria());
                var allSubscription = store.Subscriptions.Open(allId, new SubscriptionConnectionOptions());
                var allDocs = new ConcurrentBag<RavenJObject>();
                allSubscription.Subscribe(allDocs.Add);

                long usersId = store.Subscriptions.Create(new SubscriptionCriteria { KeyStartsWith = "users/" }); 
                var usersSubscription = store.Subscriptions.Open(usersId, new SubscriptionConnectionOptions());
                var usersDocs = new ConcurrentBag<RavenJObject>();
                usersSubscription.Subscribe(usersDocs.Add);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                        session.Store(new User(), "another/");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                        session.Store(new User(), "another/");
                    session.SaveChanges();
                }

                Assert.True(SpinWait.SpinUntil(() => allDocs.Count == 11, TimeSpan.FromSeconds(10)));

                Assert.Equal(11, allDocs.Count);
                Assert.Equal(1, usersDocs.Count);
            }
        }

        [Theory]
        [PropertyData("Storages")]

        public void WillAcknowledgeEmptyBatches(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                var subscriptionDocuments = store.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(0, subscriptionDocuments.Count);

                long allId = store.Subscriptions.Create(new SubscriptionCriteria());
                var allSubscription = store.Subscriptions.Open(allId, new SubscriptionConnectionOptions());
                var allDocs = new ConcurrentBag<RavenJObject>();
                allSubscription.Subscribe(allDocs.Add);

                long usersId = store.Subscriptions.Create(new SubscriptionCriteria { KeyStartsWith = "users/" });
                var usersSubscription = store.Subscriptions.Open(usersId, new SubscriptionConnectionOptions());
                var usersDocs = new ConcurrentBag<RavenJObject>();
                usersSubscription.Subscribe(usersDocs.Add);

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

                        Console.WriteLine(subscriptionDocuments[0].AckEtag + "," + subscriptionDocuments[1].AckEtag);
                        return subscriptionDocuments[0].AckEtag == subscriptionDocuments[1].AckEtag;
                    }, TimeSpan.FromSeconds(10)));

                Assert.Equal(500, allDocs.Count);
                Assert.Equal(0, usersDocs.Count);
            }
        }

        [Fact]
        public void ShouldKeepPullingDocsAfterServerRestart()
        {
            var dataPath = NewDataPath("RavenDB_2627_after_restart");

            IDocumentStore store = null;
            try
            {
                var serverDisposed = false;

                var server = GetNewServer(dataDirectory: dataPath, runInMemory: false);
                
                store = new DocumentStore()
                {
                    Url = "http://localhost:" + server.Configuration.Port,
                    DefaultDatabase = "RavenDB_2627"
                }.Initialize();

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.Store(new User());
                    session.Store(new User());
                    session.Store(new User());

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria());

                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions()
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1),
                    BatchOptions = new SubscriptionBatchOptions()
                    {
                        MaxDocCount = 1
                    }
                });
                store.Changes().WaitForAllPendingSubscriptions();

                var serverDisposingHandler = subscription.Subscribe(x =>
                {
                    server.Dispose(); // dispose the server
                    serverDisposed = true;
                });

                SpinWait.SpinUntil(() => serverDisposed, TimeSpan.FromSeconds(30));

                serverDisposingHandler.Dispose();

                var docs = new BlockingCollection<RavenJObject>();
                subscription.Subscribe(docs.Add);

                //recreate the server
                GetNewServer(dataDirectory: dataPath, runInMemory: false);
                
                RavenJObject doc;
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/arek");
                    session.SaveChanges();
                }

                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.Equal("users/arek", doc[Constants.Metadata].Value<string>("@id"));
            }
            finally
            {
                if(store != null)
                    store.Dispose();
            }
        }

        [Fact]
        public void ShouldStopPullingDocsIfThereIsNoSubscriber()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());

                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions());
                store.Changes().WaitForAllPendingSubscriptions();

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }

                var docs = new BlockingCollection<RavenJObject>();
                var subscribe = subscription.Subscribe(docs.Add);

                RavenJObject doc;
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));

                subscribe.Dispose();

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/3");
                    session.Store(new User(), "users/4");
                    session.SaveChanges();
                }

                Thread.Sleep(TimeSpan.FromSeconds(5)); // should not pull any docs because there is no subscriber that could process them

                subscription.Subscribe(docs.Add);

                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.Equal("users/3", doc[Constants.Metadata].Value<string>("@id"));

                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.Equal("users/4", doc[Constants.Metadata].Value<string>("@id"));
            }
        }

        [Fact]
        public void ShouldAllowToOpenSubscriptionIfClientDidntSentAliveNotificationOnTimeAndExceededACKTimeout()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria());

                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions()
                {
                    ClientAliveNotificationInterval = TimeSpan.FromSeconds(2),
                    BatchOptions =
                    {
                        AcknowledgmentTimeout = TimeSpan.FromSeconds(2)
                    }
                });
                store.Changes().WaitForAllPendingSubscriptions();

                subscription.BeforeAcknowledgment += () =>
                {
                    Thread.Sleep(TimeSpan.FromSeconds(2));
                    return true;
                }; // to force ACK timeout

                subscription.AfterAcknowledgment += etag => Thread.Sleep(TimeSpan.FromSeconds(20)); // to prevent the client from sending client-alive notification

                var docs = new BlockingCollection<RavenJObject>();

                subscription.Subscribe(docs.Add);
                store.Changes().WaitForAllPendingSubscriptions();

                RavenJObject _;
                Assert.True(docs.TryTake(out _, waitForDocTimeout));

                Thread.Sleep(TimeSpan.FromSeconds(10));
                
                // first open subscription didn't send the client-alive notification in time and exceeded ACK timeout, so the server should allow to open it for this subscription
                var newSubscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions());

                var docs2 = new BlockingCollection<RavenJObject>();
                newSubscription.Subscribe(docs2.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                Assert.True(docs2.TryTake(out _, waitForDocTimeout));

                Assert.False(docs.TryTake(out _, TimeSpan.FromSeconds(2))); // make sure that first subscriber didn't get new doc
            }
        }

        [Fact]
        public void CanReleaseSubscription()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                store.Subscriptions.Open(id, new SubscriptionConnectionOptions());
                store.Changes().WaitForAllPendingSubscriptions();

                Assert.Throws<SubscriptionInUseException>(() => store.Subscriptions.Open(id, new SubscriptionConnectionOptions()));

                store.Subscriptions.Release(id);

                Assert.DoesNotThrow(() => store.Subscriptions.Open(id, new SubscriptionConnectionOptions()));
            }
        }

        [Fact]
        public void ShouldPullDocumentsAfterBulkInsert()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions());
                store.Changes().WaitForAllPendingSubscriptions();

                var docs = new BlockingCollection<RavenJObject>();

                subscription.Subscribe(docs.Add);

                store.Changes().WaitForAllPendingSubscriptions();

                using (var bulk = store.BulkInsert())
                {
                    bulk.Store(new User());
                    bulk.Store(new User());
                    bulk.Store(new User());
                }

                RavenJObject doc;
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
            }
        }

        [Fact]
        public void ShouldStopPullingDocsAndCloseSubscriptionOnSubscriberErrorByDefault()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions());

                var docs = new BlockingCollection<RavenJObject>();

                var subscriberException = new TaskCompletionSource<object>();

                subscription.Subscribe(docs.Add);
                subscription.Subscribe(x =>
                {
                    throw new Exception("Fake exception");
                },
                ex => subscriberException.TrySetResult(ex));

                store.Changes().WaitForAllPendingSubscriptions();

                store.DatabaseCommands.Put("items/1", null, new RavenJObject(), new RavenJObject());

                Assert.True(subscriberException.Task.Wait(waitForDocTimeout));
                Assert.True(subscription.IsErroredBecauseOfSubscriber);
                Assert.Equal("Fake exception", subscription.LastSubscriberException.Message);

                Assert.True(SpinWait.SpinUntil(() => subscription.IsConnectionClosed, waitForDocTimeout));

                var subscriptionConfig = store.Subscriptions.GetSubscriptions(0, 1).First();

                Assert.Equal(Etag.Empty, subscriptionConfig.AckEtag);
            }
        }

        [Fact]
        public void CanSetToIgnoreSubscriberErrors()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions()
                {
                    IgnoreSubscribersErrors = true
                });
                store.Changes().WaitForAllPendingSubscriptions();

                var docs = new BlockingCollection<RavenJObject>();

                subscription.Subscribe(docs.Add);
                subscription.Subscribe(x =>
                {
                    throw new Exception("Fake exception");
                });

                store.Changes().WaitForAllPendingSubscriptions();

                store.DatabaseCommands.Put("items/1", null, new RavenJObject(), new RavenJObject());
                store.DatabaseCommands.Put("items/2", null, new RavenJObject(), new RavenJObject());

                RavenJObject doc;
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.False(subscription.IsErroredBecauseOfSubscriber);
            }
        }

        [Fact]
        public void CanUseNestedPropertiesInSubscriptionCriteria()
        {
            using (var store = NewDocumentStore())
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

                var id = store.Subscriptions.Create(new SubscriptionCriteria
                {
                    PropertiesMatch = new Dictionary<string, RavenJToken>()
                    {
                        {"Address.Street", "1st Street"}
                    },
                    PropertiesNotMatch = new Dictionary<string, RavenJToken>()
                    {
                        {"Address.ZipCode", 999}
                    }
                });

                var carolines = store.Subscriptions.Open(id, new SubscriptionConnectionOptions { BatchOptions = new SubscriptionBatchOptions { MaxDocCount = 5 } });

                var docs = new List<RavenJObject>();

                carolines.Subscribe(docs.Add);

                Assert.True(SpinWait.SpinUntil(() => docs.Count >= 5, TimeSpan.FromSeconds(60)));


                foreach (var jsonDocument in docs)
                {
                    Assert.Equal("1st Street", jsonDocument.Value<RavenJObject>("Address").Value<string>("Street"));
                }
            }
        }

        [Fact]
        public void RavenDB_3452_ShouldStopPullingDocsIfReleased()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());

                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions()
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1)
                });
                store.Changes().WaitForAllPendingSubscriptions();

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }

                var docs = new BlockingCollection<RavenJObject>();
                var subscribe = subscription.Subscribe(docs.Add);

                RavenJObject doc;

                Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                Assert.True(docs.TryTake(out doc, waitForDocTimeout));

                store.Subscriptions.Release(id);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/3");
                    session.Store(new User(), "users/4");
                    session.SaveChanges();
                }

                Thread.Sleep(TimeSpan.FromSeconds(5));

                doc = null;

                Assert.False(docs.TryTake(out doc, waitForDocTimeout), doc != null ? doc.ToString() : string.Empty);
                Assert.False(docs.TryTake(out doc, waitForDocTimeout), doc != null ? doc.ToString() : string.Empty);

                Assert.True(subscription.IsConnectionClosed);

                subscription.Dispose();
            }
        }

        [Fact]
        public void RavenDB_3453_ShouldDeserializeTheWholeDocumentsAfterTypedSubscription()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create<User>(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions());
                store.Changes().WaitForAllPendingSubscriptions();

                var users = new BlockingCollection<User>();

                subscription.Subscribe(x => users.Add(x));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Age = 31 }, "users/1");
                    session.Store(new User { Age = 27 }, "users/12");
                    session.Store(new User { Age = 25 }, "users/3");

                    session.SaveChanges();
                }

                User user;
                Assert.True(users.TryTake(out user, waitForDocTimeout));
                Assert.Equal("users/1", user.Id);
                Assert.Equal(31, user.Age);

                Assert.True(users.TryTake(out user, waitForDocTimeout));
                Assert.Equal("users/12", user.Id);
                Assert.Equal(27, user.Age);

                Assert.True(users.TryTake(out user, waitForDocTimeout));
                Assert.Equal("users/3", user.Id);
                Assert.Equal(25, user.Age);
            }
        }

        [Fact]
        public void DisposingOneSubscriptionShouldNotAffectOnNotificationsOfOthers()
        {
            using (var store = NewDocumentStore())
            {
                var id1 = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var id2 = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                var subscription1 = store.Subscriptions.Open<User>(id1, new SubscriptionConnectionOptions());
                var items1 = new BlockingCollection<User>();
                subscription1.Subscribe(items1.Add);

                var subscription2 = store.Subscriptions.Open<User>(id2, new SubscriptionConnectionOptions());
                var items2 = new BlockingCollection<User>();
                subscription2.Subscribe(items2.Add);

                store.Changes().WaitForAllPendingSubscriptions();

                using (var s = store.OpenSession())
                {
                    s.Store(new User(), "users/1");
                    s.Store(new User(), "users/2");
                    s.SaveChanges();
                }

                User user;

                Assert.True(items1.TryTake(out user, waitForDocTimeout));
                Assert.Equal("users/1", user.Id);
                Assert.True(items1.TryTake(out user, waitForDocTimeout));
                Assert.Equal("users/2", user.Id);

                Assert.True(items2.TryTake(out user, waitForDocTimeout));
                Assert.Equal("users/1", user.Id);
                Assert.True(items2.TryTake(out user, waitForDocTimeout));
                Assert.Equal("users/2", user.Id);

                subscription1.Dispose();

                using (var s = store.OpenSession())
                {
                    s.Store(new User(), "users/3");
                    s.Store(new User(), "users/4");
                    s.SaveChanges();
                }

                Assert.True(items2.TryTake(out user, waitForDocTimeout));
                Assert.Equal("users/3", user.Id);
                Assert.True(items2.TryTake(out user, waitForDocTimeout));
                Assert.Equal("users/4", user.Id);
            }
        }

        [Fact]
        public void ShouldNotBeAbleToOpenSubscriptionWhileItIsStillBeingProcessedAndAckTimeoutWasNotReachedYet()
        {
            using (var store = NewDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>() {});

                using (var subscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                {
                    ClientAliveNotificationInterval = TimeSpan.FromMilliseconds(10), // very short client-alive notification
                    BatchOptions =
                    {
                        AcknowledgmentTimeout = TimeSpan.FromHours(1) // very long ACK timeout
                    }
                }))
                {
                    var items = new BlockingCollection<User>();
                    subscription.Subscribe(x =>
                    {
                        items.Add(x);
                        Thread.Sleep(TimeSpan.FromSeconds(20));
                    });

                    store.Changes().WaitForAllPendingSubscriptions();

                    using (var s = store.OpenSession())
                    {
                        s.Store(new User());
                        s.SaveChanges();
                    }

                    Thread.Sleep(2000);

                    Assert.Throws<SubscriptionInUseException>(() => store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()));
                }
            }
        }

        [Fact]
        public void ShouldNotOverrideSubscriptionAckEtag()
        {
            using (EmbeddableDocumentStore store = NewDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria());
                var subscription = store.Subscriptions.Open(subscriptionId, new SubscriptionConnectionOptions());
                var subscriptionActions = new SubscriptionActions(store.DocumentDatabase, null);
                var names = new BlockingCollection<string>();
                var etagFirst = new Etag("01000000-0000-0001-0000-000000000001");
                var etagBigger = new Etag("01000000-0000-0001-0000-000000000003");

                store.Changes().WaitForAllPendingSubscriptions();
                subscription.Subscribe(x =>
                {
                    names.Add(x.Value<string>("Name"));
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "James" }, "users/1");
                    session.SaveChanges();
                }

                SpinWait.SpinUntil(() =>
                {
                    var updatedEtag = subscriptionActions.GetAcknowledgeEtag(subscriptionId);
                    return updatedEtag != null && etagFirst.CompareTo(updatedEtag) == 0;
                }, TimeSpan.FromSeconds(5));

                subscriptionActions.SetAcknowledgeEtag(subscriptionId, etagBigger);

                SpinWait.SpinUntil(() =>
                {
                    var updatedEtag = subscriptionActions.GetAcknowledgeEtag(subscriptionId);
                    return updatedEtag != null && etagBigger.CompareTo(updatedEtag) == 0;
                }, TimeSpan.FromSeconds(5));

                store.Changes().WaitForAllPendingSubscriptions();

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Adam" }, "users/12");
                    session.SaveChanges();
                }

                string name;
                SpinWait.SpinUntil(() =>
                {
                    names.TryTake(out name, TimeSpan.FromSeconds(1));
                    return name?.Equals("Adam") ?? false;
                }, TimeSpan.FromSeconds(5));

                var afterAnotherUpdateEtag = subscriptionActions.GetAcknowledgeEtag(subscriptionId);
                Assert.True(etagBigger.CompareTo(afterAnotherUpdateEtag) == 0);
            }
        }
    }
}
