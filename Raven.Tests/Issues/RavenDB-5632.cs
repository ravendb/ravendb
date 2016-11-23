// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1279.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5632 : ReplicationBase
    {
        [Fact]
        public void can_resolve_conflicts_using_load()
        {
            const string userDocumentId = "users/someuser";

            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" }, userDocumentId);
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" }, userDocumentId);
                    session2.SaveChanges();
                }

                ((DocumentStore)store2).RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.Url},
                            {"Database", ((DocumentStore)store2).DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>(userDocumentId);
                    Assert.Equal("AnotherName Name", user.Name);
                }
            }
        }

        [Fact]
        public async Task can_resolve_conflicts_using_load_async()
        {
            const string userDocumentId = "users/someuser";

            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" }, userDocumentId);
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" }, userDocumentId);
                    session2.SaveChanges();
                }

                ((DocumentStore)store2).RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.Url},
                            {"Database", ((DocumentStore)store2).DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(userDocumentId);
                    Assert.Equal("AnotherName Name", user.Name);
                }
            }
        }

        [Fact]
        public void can_resolve_conflicts_using_load_embeddale_store()
        {
            const string userDocumentId = "users/someuser";

            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" }, userDocumentId);
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" }, userDocumentId);
                    session2.SaveChanges();
                }

                store2.RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.HttpServer.SystemDatabase.ServerUrl},
                            {"Database", store2.DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>(userDocumentId);
                    Assert.Equal("AnotherName Name", user.Name);
                }
            }
        }

        [Fact]
        public async Task can_resolve_conflicts_using_load_embeddale_store_async()
        {
            const string userDocumentId = "users/someuser";

            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" }, userDocumentId);
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" }, userDocumentId);
                    session2.SaveChanges();
                }

                store2.RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.HttpServer.SystemDatabase.ServerUrl},
                            {"Database", store2.DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(userDocumentId);
                    Assert.Equal("AnotherName Name", user.Name);
                }
            }
        }

        [Fact]
        public void can_resolve_conflicts_using_query()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" });
                    session1.Store(new User { Name = "Name2" });
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" });
                    session2.Store(new User { Name = "AnotherName2" });
                    session2.SaveChanges();
                }

                ((DocumentStore)store2).RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();
                    conflictObserver.DesiredConflicts = 3;

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.Url},
                            {"Database", ((DocumentStore)store2).DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenSession())
                {
                    var users = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(2, users.Count);
                    Assert.True(users[0].Name == "AnotherName Name" || users[0].Name == "AnotherName2 Name2");
                    Assert.True(users[1].Name == "AnotherName Name" || users[1].Name == "AnotherName2 Name2");
                }
            }
        }

        [Fact]
        public async Task can_resolve_conflicts_using_async_document_query()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" });
                    session1.Store(new User { Name = "Name2" });
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" });
                    session2.Store(new User { Name = "AnotherName2" });
                    session2.SaveChanges();
                }

                ((DocumentStore)store2).RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();
                    conflictObserver.DesiredConflicts = 3;

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.Url},
                            {"Database", ((DocumentStore)store2).DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var users = await session.Advanced.AsyncLuceneQuery<User>()
                        .WaitForNonStaleResults()
                        .ToListAsync();
                    Assert.Equal(2, users.Count);
                    Assert.True(users[0].Name == "AnotherName Name" || users[0].Name == "AnotherName2 Name2");
                    Assert.True(users[1].Name == "AnotherName Name" || users[1].Name == "AnotherName2 Name2");
                }
            }
        }

        [Fact]
        public void can_resolve_conflicts_using_query_embeddale_store()
        {
            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" });
                    session1.Store(new User { Name = "Name2" });
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" });
                    session2.Store(new User { Name = "AnotherName2" });
                    session2.SaveChanges();
                }

                store2.RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();
                    conflictObserver.DesiredConflicts = 3;

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.HttpServer.SystemDatabase.ServerUrl},
                            {"Database", store2.DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenSession())
                {
                    var users = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(2, users.Count);
                    Assert.True(users[0].Name == "AnotherName Name" || users[0].Name == "AnotherName2 Name2");
                    Assert.True(users[1].Name == "AnotherName Name" || users[1].Name == "AnotherName2 Name2");
                }
            }
        }

        [Fact]
        public async Task can_resolve_conflicts_using_async_document_query_embeddale_store()
        {
            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" });
                    session1.Store(new User { Name = "Name2" });
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" });
                    session2.Store(new User { Name = "AnotherName2" });
                    session2.SaveChanges();
                }

                store2.RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();
                    conflictObserver.DesiredConflicts = 3;

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.HttpServer.SystemDatabase.ServerUrl},
                            {"Database", store2.DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var users = await session.Advanced.AsyncLuceneQuery<User>()
                        .WaitForNonStaleResults()
                        .ToListAsync();
                    Assert.Equal(2, users.Count);
                    Assert.True(users[0].Name == "AnotherName Name" || users[0].Name == "AnotherName2 Name2");
                    Assert.True(users[1].Name == "AnotherName Name" || users[1].Name == "AnotherName2 Name2");
                }
            }
        }

        [Fact]
        public void can_resolve_conflicts_using_load_starting_with()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" });
                    session1.Store(new User { Name = "Name2" });
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" });
                    session2.Store(new User { Name = "AnotherName2" });
                    session2.SaveChanges();
                }

                ((DocumentStore)store2).RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();
                    conflictObserver.DesiredConflicts = 3;

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.Url},
                            {"Database", ((DocumentStore)store2).DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenSession())
                {
                    var users = session.Advanced.LoadStartingWith<User>("users/");
                    Assert.Equal("AnotherName Name", users[0].Name);
                    Assert.Equal("AnotherName2 Name2", users[1].Name);
                }
            }
        }

        [Fact]
        public void can_resolve_conflicts_using_load_starting_with_embeddale_store()
        {
            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" });
                    session1.Store(new User { Name = "Name2" });
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" });
                    session2.Store(new User { Name = "AnotherName2" });
                    session2.SaveChanges();
                }

                store2.RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();
                    conflictObserver.DesiredConflicts = 3;

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.HttpServer.SystemDatabase.ServerUrl},
                            {"Database", store2.DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenSession())
                {
                    var users = session.Advanced.LoadStartingWith<User>("users/");
                    Assert.Equal("AnotherName Name", users[0].Name);
                    Assert.Equal("AnotherName2 Name2", users[1].Name);
                }
            }
        }

        [Fact]
        public async Task can_resolve_conflicts_using_load_starting_with_async()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenAsyncSession())
                using (var session2 = store2.OpenAsyncSession())
                {
                    await session1.StoreAsync(new User { Name = "Name" });
                    await session1.StoreAsync(new User { Name = "Name2" });
                    await session1.SaveChangesAsync();

                    await session2.StoreAsync(new User { Name = "AnotherName" });
                    await session2.StoreAsync(new User { Name = "AnotherName2" });
                    await session2.SaveChangesAsync();
                }

                ((DocumentStore)store2).RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();
                    conflictObserver.DesiredConflicts = 3;

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.Url},
                            {"Database", ((DocumentStore)store2).DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var users = (await session.Advanced.LoadStartingWithAsync<User>("users/")).ToList();
                    Assert.Equal("AnotherName Name", users[0].Name);
                    Assert.Equal("AnotherName2 Name2", users[1].Name);
                }
            }
        }

        [Fact]
        public async Task can_resolve_conflicts_using_load_starting_with_async_embeddale_store()
        {
            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User { Name = "Name" });
                    session1.Store(new User { Name = "Name2" });
                    session1.SaveChanges();

                    session2.Store(new User { Name = "AnotherName" });
                    session2.Store(new User { Name = "AnotherName2" });
                    session2.SaveChanges();
                }

                store2.RegisterListener(new MergeConflicts());

                var changes = store2.Changes();
                using (var conflictObserver = new ConflictObserver())
                using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
                {
                    changes.WaitForAllPendingSubscriptions();
                    conflictObserver.DesiredConflicts = 3;

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.HttpServer.SystemDatabase.ServerUrl},
                            {"Database", store2.DefaultDatabase},
                            {"TransitiveReplicationBehavior", "Replicate"}
                        }
                    };

                    store1.DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                        null, new RavenJObject
                        {
                            {
                                "Destinations", new RavenJArray(destinations)
                            }
                        }, new RavenJObject());

                    conflictObserver.WaitOne();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var users = (await session.Advanced.LoadStartingWithAsync<User>("users/")).ToList();
                    Assert.Equal("AnotherName Name", users[0].Name);
                    Assert.Equal("AnotherName2 Name2", users[1].Name);
                }
            }
        }

        private class MergeConflicts : IDocumentConflictListener
        {
            public bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument)
            {
                resolvedDocument = new JsonDocument
                {
                    DataAsJson = new RavenJObject
                    {
                        {"Name", string.Join(" ", conflictedDocs.Select(x => x.DataAsJson.Value<string>("Name")).OrderBy(x => x))}
                    },
                    Metadata = conflictedDocs.First().Metadata
                };

                resolvedDocument.Metadata.Remove("@id");
                resolvedDocument.Metadata.Remove("@etag");

                return true;
            }
        }

        private class User
        {
            public string Name { get; set; }
        }

        private class ConflictObserver : IObserver<ReplicationConflictNotification>, IDisposable
        {
            private AutoResetEvent NewConflictEvent { get; set; }
            public int DesiredConflicts { private get; set; }
            private int ConflictsCount { get; set; }

            public ConflictObserver()
            {
                NewConflictEvent = new AutoResetEvent(false);
                DesiredConflicts = 1;
            }

            public void OnNext(ReplicationConflictNotification value)
            {
                if (DesiredConflicts == ++ConflictsCount)
                    NewConflictEvent.Set();
            }

            public void OnError(Exception error)
            {
            }

            public void OnCompleted()
            {
            }

            public void Dispose()
            {
                NewConflictEvent.Dispose();
            }

            public void WaitOne()
            {
                NewConflictEvent.WaitOne();
            }
        }
    }
}