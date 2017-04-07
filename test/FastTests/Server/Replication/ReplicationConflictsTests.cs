using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.NotificationCenter;
using Raven.Server.Utils;
using Xunit;
using Constants = Raven.Client.Constants;

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

        private class New_User: User { }

        private class New_User2: User { }

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

            Assert.Equal(ReplicationUtils.ConflictStatus.AlreadyMerged, ReplicationUtils.GetConflictStatus(remote, local));
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

            Assert.Equal(ReplicationUtils.ConflictStatus.Update, ReplicationUtils.GetConflictStatus(remote, local));
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

            Assert.Equal(ReplicationUtils.ConflictStatus.Conflict, ReplicationUtils.GetConflictStatus(remote, local));
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

            Assert.Equal(ReplicationUtils.ConflictStatus.Conflict, ReplicationUtils.GetConflictStatus(remote, local));
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

            Assert.Equal(ReplicationUtils.ConflictStatus.Update, ReplicationUtils.GetConflictStatus(remote, local));
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

            Assert.Equal(ReplicationUtils.ConflictStatus.Conflict, ReplicationUtils.GetConflictStatus(remote, local));
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

            Assert.Equal(ReplicationUtils.ConflictStatus.AlreadyMerged, ReplicationUtils.GetConflictStatus(remote, local));
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

            Assert.Equal(ReplicationUtils.ConflictStatus.Conflict, ReplicationUtils.GetConflictStatus(remote, local));
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
                Assert.Equal(2, conflicts.Results.Length);
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

                Assert.Equal(2, WaitUntilHasConflict(store2, "users/3").Results.Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "users/2").Results.Length);
                // conflict between two tombstones, resolved automaticlly to tombstone.
                var tombstones = WaitUntilHasTombstones(store2);
                Assert.Equal("users/1", tombstones.Single());
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

                var operation = store2.Operations.Send(new DeleteByIndexOperation(userIndex.IndexName, new IndexQuery() { Query = string.Empty }));

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
                var operation = store2.Operations.Send(new PatchByIndexOperation(userIndex.IndexName, new IndexQuery()
                {
                    Query = string.Empty
                }, new PatchRequest
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

                var exception = Assert.Throws<DocumentConflictException>(() => store2.Operations.Send(new PatchOperation("foo/bar", null, new PatchRequest
                {
                    Script = "this.x = 123"
                })));

                Assert409Response(exception);
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

                Assert.Equal(3, WaitUntilHasConflict(store3, "foo/bar", 3).Results.Length);
            }
        }

        [Fact]
        public void Conflict_should_be_created_for_document_in_different_collections()
        {
            const string dbName1 = "FooBar-1";
            const string dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test1" }, "foo/bar");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new New_User { Name = "test1" }, "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);
            }
        }

        [Fact]
        public void Conflict_should_be_created_and_resolved_for_document_in_different_collections()
        {
            const string dbName1 = "FooBar-1";
            const string dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test1" }, "foo/bar");
                    s2.SaveChanges();
                }

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new New_User { Name = "test1" }, "foo/bar");
                    s1.SaveChanges();
                }

                SetReplicationConflictResolution(store2, StraightforwardConflictResolution.ResolveToLatest);
                SetupReplication(store1, store2);

                var newCollection = WaitForValue(() =>
                {
                    using (var s2 = store2.OpenSession())
                    {
                        var metadata = s2.Advanced.GetMetadataFor(s2.Load<User>("foo/bar"));
                        var collection = metadata.GetString(Constants.Documents.Metadata.Collection);
                        return collection;
                    }
                }, "New_Users");

                Assert.Equal("New_Users", newCollection);
            }
        }

        [Fact]
        public void Conflict_should_be_resolved_for_document_in_different_collections_after_setting_new_resolution()
        {
            const string dbName1 = "FooBar-1";
            const string dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test1" }, "foo/bar");
                    s2.SaveChanges();
                }

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new New_User { Name = "test1" }, "foo/bar");
                    s1.SaveChanges();
                }

                SetupReplication(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar", 2);

                SetReplicationConflictResolution(store2, StraightforwardConflictResolution.ResolveToLatest);

                var count = WaitForValue(() => GetConflicts(store2, "foo/bar").Results.Length, 0);
                Assert.Equal(count, 0);

                var newCollection = WaitForValue(() =>
                {
                    using (var s2 = store2.OpenSession())
                    {
                        var metadata = s2.Advanced.GetMetadataFor(s2.Load<User>("foo/bar"));
                        var collection = metadata.GetString(Constants.Documents.Metadata.Collection);
                        return collection;
                    }
                }, "New_Users");

                Assert.Equal("New_Users", newCollection);
            }
        }

        [Fact]
        public void Conflict_should_be_resolved_for_document_in_different_collections_after_saving_in_new_collection()
        {
            const string dbName1 = "FooBar-1";
            const string dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test1" }, "foo/bar");
                    s2.SaveChanges();
                }

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new New_User { Name = "test1" }, "foo/bar");
                    s1.SaveChanges();
                }

                SetupReplication(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar", 2);

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new New_User2 { Name = "test2" }, "foo/bar");
                    s2.SaveChanges();
                }

                var count = WaitForValue(() => GetConflicts(store2, "foo/bar").Results.Length, 0);
                Assert.Equal(count, 0);

                New_User2 newDoc = null;
                var newCollection = WaitForValue(() =>
                {
                    using (var s2 = store2.OpenSession())
                    {
                        newDoc = s2.Load<New_User2>("foo/bar");
                        var metadata = s2.Advanced.GetMetadataFor(newDoc);
                        var collection = metadata.GetString(Constants.Documents.Metadata.Collection);
                        return collection;
                    }
                }, "New_User2s");

                Assert.Equal("New_User2s", newCollection);
                Assert.Equal("test2", newDoc.Name);
            }
        }

        [Fact]
        public void Should_not_resolve_conflcit_with_script_when_they_from_different_collection()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();

            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "foo/bar");
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Store(new New_User { Name = "Oren" }, "foo/bar");
                session.SaveChanges();
            }

            SetScriptResolution(store2, "return {Name:docs[0].Name + '123'};","Users");
            SetupReplication(store1, store2);

            var db2 = GetDocumentDatabaseInstanceFor(store2).Result.NotificationCenter;
           
            Assert.Equal(1, WaitForValue(() => db2.GetAlertCount(), 1));

            IEnumerable<NotificationTableValue> alerts;
            using (db2.GetStored(out alerts))
            {
                var alertsList = alerts.ToList();
                string msg;
                alertsList[0].Json.TryGet("Message", out msg);
                Assert.True(msg.Contains("All conflicted documents must have same collection name, but we found conflicted document in Users and an other one in New_Users"));
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

