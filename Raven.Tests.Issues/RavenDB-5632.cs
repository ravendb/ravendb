// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1279.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5632 : ReplicationBase
    {
        const string DefaultDatabase = "ReplicatingDatabase";

        [Fact]
        public void can_resolve_conflicts_using_load()
        {
            const string userDocumentId = "users/someuser";

            using (var store1 = CreateStore(databaseName: DefaultDatabase))
            using (var store2 = CreateStore(databaseName: DefaultDatabase))
            {
                store1.Initialize();
                store2.Initialize();

                using (var session1 = store1.OpenSession())
                using (var session2 = store2.OpenSession())
                {
                    session1.Store(new User {Name = "Name"}, userDocumentId);
                    session1.SaveChanges();

                    session2.Store(new User {Name = "AnotherName"}, userDocumentId);
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
                            {"Url", store2.Url},
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
                    Assert.Equal(user.Name, "AnotherName Name");
                }
            }
        }

        [Fact]
        public void can_resolve_conflicts_using_query()
        {
            using (var store1 = CreateStore(databaseName: DefaultDatabase))
            using (var store2 = CreateStore(databaseName: DefaultDatabase))
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
                    conflictObserver.DesiredConflicts = 2;

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.Url},
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
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Equal(2, users.Count);
                    Assert.True(users[0].Name == "AnotherName Name" || users[0].Name == "AnotherName2 Name2");
                    Assert.True(users[1].Name == "AnotherName Name" || users[1].Name == "AnotherName2 Name2");
                }
            }
        }

        [Fact]
        public void can_resolve_conflicts_using_load_starting_with()
        {
            using (var store1 = CreateStore(databaseName: DefaultDatabase))
            using (var store2 = CreateStore(databaseName: DefaultDatabase))
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
                    conflictObserver.DesiredConflicts = 2;

                    var destinations = new List<RavenJObject>
                    {
                        new RavenJObject
                        {
                            {"Url", store2.Url},
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
                    Assert.Equal(users[0].Name, "AnotherName Name");
                    Assert.Equal(users[1].Name, "AnotherName2 Name2");
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
            private AutoResetEvent NewConflictEvent { get; } = new AutoResetEvent(false);
            public int DesiredConflicts { private get; set; } = 1;
            private int ConflictsCount { get; set; }

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
