using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Replication.Messages;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases.Documents;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Replication;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationConflictsTests : ReplicationTestsBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public void All_remote_etags_lower_than_local_should_return_AlreadyMerged_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 11 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 12 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.AlreadyMerged, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void All_local_etags_lower_than_remote_should_return_Update_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Update, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Some_remote_etags_lower_than_local_and_some_higher_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 75 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 95 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 2 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Some_remote_etags_lower_than_local_and_some_higher_should_return_Conflict_at_conflict_status_with_different_order()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 75 },
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 95 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_larger_size_than_local_should_return_Update_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 40 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Update, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_with_different_dbId_set_than_local_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() };
            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 10 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_smaller_than_local_and_all_remote_etags_lower_than_local_should_return_AlreadyMerged_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
                new ChangeVectorEntry { DbId = dbIds[3], Etag = 40 }
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.AlreadyMerged, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_smaller_than_local_and_some_remote_etags_higher_than_local_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3000 },
                new ChangeVectorEntry { DbId = dbIds[3], Etag = 40 }
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 100 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 200 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 300 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }


        [Fact]
        public void Conflict_same_time_with_master_slave()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test" }, "foo/bar");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test2" }, "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                var conflicts = WaitUntilHasConflict(store2, "foo/bar");
                Assert.Equal(2, conflicts["foo/bar"].Count);
            }
        }

        [Fact]
        public void Conflict_insensitive_check()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test" }, "users/1");
                    s1.Store(new User { Name = "test" }, "users/2");
                    s1.Store(new User { Name = "test" }, "users/3");
                    s1.SaveChanges();
                    s1.Delete("users/1");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test2" }, "Users/1");
                    s2.Store(new User { Name = "test2" }, "Users/2");
                    s2.Store(new User { Name = "test2" }, "Users/3");
                    s2.SaveChanges();
                    s2.Delete("Users/1");
                    s2.Delete("Users/2");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                var conflicts = WaitUntilHasConflict(store2, "users/3");
                Assert.Equal(2, conflicts["users/3"].Count);
                conflicts = WaitUntilHasConflict(store2, "users/2");
                Assert.Equal(2, conflicts["users/2"].Count);
                // conflict between two tombstones, resolved automaticlly to tombstone.
                Assert.Equal(2, WaitUntilHasTombstones(store2, 2).Count);
            }
        }

        [Fact]
        public void Conflict_then_data_query_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test" }, "foo/bar");
                    s1.SaveChanges();
                }

                var userIndex = new UserIndex();
                store2.ExecuteIndex(userIndex);

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test2" }, "foo/bar");
                    s2.SaveChanges();
                }
                WaitForIndexing(store2);
                SetupReplication(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar");

                // /indexes/Raven/DocumentsByEntityName
                using (var session = store2.OpenSession())
                {
                    var exception = Assert.Throws<DocumentConflictException>(() => session.Query<User>(userIndex.IndexName).ToList());
                    Assert409Response(exception);
                }
            }
        }

        [Fact]
        public void Conflict_then_delete_query_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test" }, "foo/bar");
                    s1.SaveChanges();
                }

                var userIndex = new UserIndex();
                store2.ExecuteIndex(userIndex);

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test2" }, "foo/bar");
                    s2.SaveChanges();
                }
                WaitForIndexing(store2);
                SetupReplication(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar");

                var operation = store2.Operations.Send(new DeleteByIndexOperation(userIndex.IndexName, new IndexQuery(store1.Conventions) { Query = string.Empty }));

                Assert.Throws<DocumentConflictException>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(15)));
            }
        }

        [Fact]
        public void Conflict_then_patching_query_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test" }, "foo/bar");
                    s1.SaveChanges();
                }

                var userIndex = new UserIndex();
                store2.ExecuteIndex(userIndex);

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test2" }, "foo/bar");
                    s2.SaveChanges();
                }
                WaitForIndexing(store2);
                SetupReplication(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar");

                // /indexes/Raven/DocumentsByEntityName
                var operation = store2.Operations.Send(new PatchByIndexOperation(userIndex.IndexName, new IndexQuery(store1.Conventions)
                {
                    Query = string.Empty
                }, new Raven.NewClient.Client.Data.PatchRequest
                {
                    Script = string.Empty
                }));

                Assert.Throws<DocumentConflictException>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(15)));
            }
        }

        [Fact]
        public void Conflict_then_load_by_id_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test" }, "foo/bar");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test2" }, "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar");

                using (var session = store2.OpenSession())
                {
                    var exception = Assert.Throws<DocumentConflictException>(() => session.Load<User>("foo/bar"));
                    Assert409Response(exception);
                }
            }
        }

        [Fact]
        public void Conflict_then_patch_request_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test" }, "foo/bar");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test2" }, "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar");

                var exception = Assert.Throws<DocumentConflictException>(() => store2.Operations.Send(new PatchOperation("foo/bar", null, new Raven.NewClient.Client.Data.PatchRequest
                {
                    Script = "this.x = 123"
                })));

                Assert409Response(exception);
            }
        }

        [Fact]
        public void Conflict_then_delete_request_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test" }, "foo/bar");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test2" }, "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar");

                using (var session = store2.OpenSession())
                {
                    var exception = Assert.Throws<DocumentConflictException>(() =>
                    {
                        session.Delete("foo/bar");
                        session.SaveChanges();
                    });
                    Assert409Response(exception);
                }
            }
        }

        private static void Assert409Response(DocumentConflictException e)
        {
            Assert.NotNull(e);
            Assert.Equal("foo/bar", e.DocId);
        }

        [Fact]
        public void Conflict_should_work_on_master_slave_slave()
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            var dbName3 = "FooBar-3";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            using (var store3 = GetDocumentStore(dbSuffixIdentifier: dbName3))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test1" }, "foo/bar");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test2" }, "foo/bar");
                    s2.SaveChanges();
                }
                using (var s3 = store3.OpenSession())
                {
                    s3.Store(new User { Name = "test3" }, "foo/bar");
                    s3.SaveChanges();
                }

                SetupReplication(store1, store3);
                SetupReplication(store2, store3);

                var conflicts = WaitUntilHasConflict(store3, "foo/bar", 3);

                Assert.Equal(3, conflicts["foo/bar"].Count);
            }
        }



        private class UserIndex : AbstractIndexCreationTask<User>
        {
            public UserIndex()
            {
                Map = users => from u in users
                               select new User
                               {
                                   Id = u.Id,
                                   Name = u.Name,
                                   Age = u.Age
                               };

                Index(x => x.Name, FieldIndexing.Analyzed);

                Analyze(x => x.Name, typeof(RavenStandardAnalyzer).FullName);

            }
        }
    }
}

