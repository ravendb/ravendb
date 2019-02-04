using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Replication;
using Raven.Client.Exceptions.Documents;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.NotificationCenter;
using Raven.Server.Utils;
using Xunit;
using Constants = Raven.Client.Constants;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;

namespace SlowTests.Server.Replication
{
    public class ReplicationConflictsTests : ReplicationTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        private class New_User : User { }

        private class New_User2 : User { }

        [Fact]
        public void All_remote_etags_lower_than_local_should_return_AlreadyMerged_at_conflict_status()
        {
            var dbIds = new List<string> { new string('1', 22), new string('2', 22), new string('3', 22) };
            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 0},
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 11, NodeTag = 1},
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 12, NodeTag = 2},
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3, NodeTag = 2 },
            };

            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(remote.SerializeVector(), local.SerializeVector()));
        }

        [Fact]
        public void All_local_etags_lower_than_remote_should_return_Update_at_conflict_status()
        {
            var dbIds = new List<string> { new string('1', 22), new string('2', 22), new string('3', 22) };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3, NodeTag = 2 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30, NodeTag = 2 },
            };

            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote.SerializeVector(), local.SerializeVector()));
        }

        [Fact]
        public void Some_remote_etags_lower_than_local_and_some_higher_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<string> { new string('1', 22), new string('2', 22), new string('3', 22) };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 75, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3, NodeTag = 2 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 95, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 2, NodeTag = 2 },
            };

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote.SerializeVector(), local.SerializeVector()));
        }

        [Fact]
        public void Some_remote_etags_lower_than_local_and_some_higher_should_return_Conflict_at_conflict_status_with_different_order()
        {
            var dbIds = new List<string> { new string('1', 22), new string('2', 22), new string('3', 22) };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 75, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3, NodeTag = 2 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 95, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 2, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 2 },
            };

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote.SerializeVector(), local.SerializeVector()));
        }

        [Fact]
        public void Remote_change_vector_larger_size_than_local_should_return_Update_at_conflict_status()
        {
            var dbIds = new List<string> { new string('1', 22), new string('2', 22), new string('3', 22) };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30, NodeTag = 2 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30, NodeTag = 2 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 40, NodeTag = 2 }
            };

            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote.SerializeVector(), local.SerializeVector()));
        }

        [Fact]
        public void Remote_change_vector_with_different_dbId_set_than_local_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<string> { new string('1', 22), new string('2', 22), new string('3', 22) };
            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 0 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 10, NodeTag = 0 }
            };

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote.SerializeVector(), local.SerializeVector()));
        }

        [Fact]
        public void Remote_change_vector_smaller_than_local_and_all_remote_etags_lower_than_local_should_return_AlreadyMerged_at_conflict_status()
        {
            var dbIds = new List<string> { new string('1', 22), new string('2', 22), new string('3', 22), new string('4', 22) };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30, NodeTag = 2 },
                new ChangeVectorEntry { DbId = dbIds[3], Etag = 40, NodeTag = 3 }
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3, NodeTag = 2 }
            };

            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(remote.SerializeVector(), local.SerializeVector()));
        }

        [Fact]
        public void Remote_change_vector_smaller_than_local_and_some_remote_etags_higher_than_local_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<string> { new string('1', 22), new string('2', 22), new string('3', 22), new string('4', 22) };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3000, NodeTag = 2 },
                new ChangeVectorEntry { DbId = dbIds[3], Etag = 40, NodeTag = 3 }
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 100, NodeTag = 0 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 200, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 300, NodeTag = 2 }
            };

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote.SerializeVector(), local.SerializeVector()));
        }


        [Fact]
        public async Task Conflict_same_time_with_master_slave()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo1",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo2",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await SetupReplicationAsync(store1, store2);

                var conflicts = WaitUntilHasConflict(store2, "foo/bar");
                Assert.Equal(2, conflicts.Length);
            }
        }

        [Fact]
        public async Task DatabaseChangeVectorShouldBeIdentical()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await SetupReplicationAsync(store1, store2);
                var conflicts = WaitUntilHasConflict(store2, "foo/bar");
                Assert.Equal(2, conflicts.Length);

                await SetupReplicationAsync(store2, store1);
                conflicts = WaitUntilHasConflict(store1, "foo/bar");
                Assert.Equal(2, conflicts.Length);

                var db1 = await GetDatabase(store1.Database);
                var db2 = await GetDatabase(store2.Database);
                using (db1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx1))
                using (db2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx2))
                using (ctx1.OpenReadTransaction())
                using (ctx2.OpenReadTransaction())
                {
                    var cv1 = DocumentsStorage.GetDatabaseChangeVector(ctx1).ToChangeVector().OrderByDescending(x => x);
                    var cv2 = DocumentsStorage.GetDatabaseChangeVector(ctx2).ToChangeVector().OrderByDescending(x => x);
                    Assert.True(cv1.SequenceEqual(cv2));
                }
            }
        }

        [Fact]
        public async Task Conflict_insensitive_check()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo1",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo2",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await SetupReplicationAsync(store1, store2);

                Assert.Equal(2, WaitUntilHasConflict(store2, "users/3").Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "users/2").Length);
                // conflict between two tombstones, resolved automaticlly to tombstone.
                var tombstones = WaitUntilHasTombstones(store2);
                Assert.Equal("users/1", tombstones.Single());
            }
        }

        [Fact]
        public async Task Conflict_then_data_query_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo1",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo2",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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
                await SetupReplicationAsync(store1, store2);

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
        public async Task Conflict_then_delete_query_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo1",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo2",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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
                await SetupReplicationAsync(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar");

                var operation = store2.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = $"FROM INDEX '{userIndex.IndexName}'" }));

                Assert.Throws<DocumentConflictException>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(15)));
            }
        }

        [Fact]
        public async Task Conflict_then_patching_query_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo1",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo2",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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
                await SetupReplicationAsync(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar");

                // /indexes/Raven/DocumentsByEntityName
                var operation = store2.Operations.Send(new PatchByQueryOperation(new IndexQuery
                {
                    Query = $"FROM INDEX '{userIndex.IndexName}' UPDATE {{ }}"
                }));

                Assert.Throws<DocumentConflictException>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(15)));
            }
        }

        [Fact]
        public async Task Conflict_then_load_by_id_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo1",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo2",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await SetupReplicationAsync(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar");

                using (var session = store2.OpenSession())
                {
                    var exception = Assert.Throws<DocumentConflictException>(() => session.Load<User>("foo/bar"));
                    Assert409Response(exception);
                }
            }
        }

        [Fact]
        public async Task Conflict_then_patch_request_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo1",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo2",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await SetupReplicationAsync(store1, store2);

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
        public async Task Conflict_should_work_on_master_slave_slave()
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            var dbName3 = "FooBar-3";
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store3 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName3}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await SetupReplicationAsync(store1, store3);
                await SetupReplicationAsync(store2, store3);

                Assert.Equal(3, WaitUntilHasConflict(store3, "foo/bar", 3).Length);
            }
        }

        [Fact]
        public async Task Conflict_should_be_created_for_document_in_different_collections()
        {
            const string dbName1 = "FooBar-1";
            const string dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await SetupReplicationAsync(store1, store2);

                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Length);
            }
        }

        [Fact]
        public async Task Conflict_should_be_created_and_resolved_for_document_in_different_collections()
        {
            const string dbName1 = "FooBar-1";
            const string dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await SetReplicationConflictResolutionAsync(store2, StraightforwardConflictResolution.ResolveToLatest);
                await SetupReplicationAsync(store1, store2);

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
        public async Task Conflict_should_be_resolved_for_document_in_different_collections_after_setting_new_resolution()
        {
            const string dbName1 = "FooBar-1";
            const string dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await SetupReplicationAsync(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar", 2);

                await SetReplicationConflictResolutionAsync(store2, StraightforwardConflictResolution.ResolveToLatest);

                var count = WaitForValue(() => store2.Commands().GetConflictsFor("foo/bar").Length, 0);
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
        public async Task Conflict_should_be_resolved_for_document_in_different_collections_after_saving_in_new_collection()
        {
            const string dbName1 = "FooBar-1";
            const string dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await SetupReplicationAsync(store1, store2);

                WaitUntilHasConflict(store2, "foo/bar", 2);

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new New_User2 { Name = "test2" }, "foo/bar");
                    s2.SaveChanges();
                }

                var count = WaitForValue(() => store2.Commands().GetConflictsFor("foo/bar").Length, 0);
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
        public async Task Should_not_resolve_conflcit_with_script_when_they_from_different_collection()
        {
            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
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

                await SetScriptResolutionAsync(store2, "return {Name:docs[0].Name + '123'};", "Users");
                await SetupReplicationAsync(store1, store2);
                var db2 = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store2.Database).Result.NotificationCenter;

                Assert.Equal(1, WaitForValue(() => db2.GetAlertCount(), 1));

                IEnumerable<NotificationTableValue> alerts;
                using (db2.GetStored(out alerts))
                {
                    var alertsList = alerts.ToList();
                    string msg;
                    alertsList[0].Json.TryGet("Message", out msg);
                    Assert.True(msg.Contains(
                        "All conflicted documents must have same collection name, but we found conflicted document in Users and an other one in New_Users"));
                }
            }
        }

        [Fact]
        public async Task ConflictOnEmptyCollection()
        {
            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                },
            }))
            using (var store2 = GetDocumentStore())
            {
                
                using (var session = store1.OpenSession())
                {
                    var user = new User
                    {
                        Name = "Karmel"
                    };
                    session.Store(user, "test");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata["@collection"] = null;
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    var user = new User
                    {
                        Name = "Oren"
                    };
                    session.Store(user, "test");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata["@collection"] = null;
                    session.SaveChanges();
                }
                await SetupReplicationAsync(store2, store1);
                var db1 = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result.DocumentsStorage.ConflictsStorage;

                Assert.Equal(2, WaitForValue(() => db1.ConflictsCount, 2));
            }
        }

        [Fact]
        public async Task ExistingConflictShouldNotReflectOnOtherDocuments()
        {
            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                },
            }))
            using (var store2 = GetDocumentStore())
            {

                using (var session = store1.OpenSession())
                {
                    var user = new User
                    {
                        Name = "Karmel"
                    };
                    session.Store(user, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    var user = new User
                    {
                        Name = "Oren"
                    };
                    session.Store(user, "foo/bar");
                    session.SaveChanges();
                }
                await SetupReplicationAsync(store2, store1);
                var db1 = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result.DocumentsStorage.ConflictsStorage;
                Assert.Equal(2, WaitForValue(() => db1.ConflictsCount, 2));


                using (var session = store1.OpenSession())
                {
                    var user = new User
                    {
                        Name = "John"
                    };
                    session.Store(user, "test");
                    session.SaveChanges();

                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.False(metadata.Keys.Contains(Constants.Documents.Metadata.Flags));
                }
            }
        }

        [Fact]
        public async Task BackgroundResolveToLatestInCluster()
        {
            var leader1 = await CreateRaftClusterAndGetLeader(3);

            using (var store1 = GetDocumentStore(new Options
            {
                Server = leader1,
                ReplicationFactor = 3,
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                Server = leader1,
                ReplicationFactor = 1
            }))
            {
                await SetupReplicationAsync(store2, store1);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Length);

                await SetReplicationConflictResolutionAsync(store1, StraightforwardConflictResolution.ResolveToLatest);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Grisha"));
                
                var database = Servers.Single(s => s.WebUrl == store1.Urls[0]).ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    Assert.Equal(3, count);
                }
            }
        }

        [Fact]
        public async Task ResolveToLatestInClusterOnTheFly()
        {
            var leader1 = await CreateRaftClusterAndGetLeader(3);

            using (var store1 = GetDocumentStore(new Options
            {
                Server = leader1,
                ReplicationFactor = 3
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                Server = leader1,
                ReplicationFactor = 1
            }))
            {
                await SetupReplicationAsync(store2, store1);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }
                WaitForUserToContinueTheTest(store1);
                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Grisha"));

                var database = Servers.Single(s => s.WebUrl == store1.Urls[0]).ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    Assert.Equal(3, count);
                }

            }
        }

        [Fact]
        public async Task ResolveToLatestInClusterWithCounter()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store2, store1);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }
                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Grisha"));
                var database = Servers.Single(s => s.WebUrl == store1.Urls[0]).ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    Assert.Equal(3, count);
                }

                WaitForUserToContinueTheTest(store1);

                using (var session = store1.OpenAsyncSession())
                {
                    session.CountersFor("foo/bar").Increment("likes",10);
                    await session.SaveChangesAsync();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    Assert.Equal(3, count);
                }
            }
        }

        [Fact]
        public async Task ResolveToLatestInClusterWithAttachment()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store2, store1);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }
                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Grisha"));
                var database = Servers.Single(s => s.WebUrl == store1.Urls[0]).ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    Assert.Equal(3, count);
                }

                WaitForUserToContinueTheTest(store1);

                using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    store1.Operations.Send(new PutAttachmentOperation("foo/bar", "a1", a1, "a2/jpeg"));
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    Assert.Equal(3, count);
                }
            }
        }

        [Fact]
        public async Task ModifyResolvedDocumentShouldKeepTheRevisions()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store2, store1);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Grisha"));
                using (var session = store1.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<User>("foo/bar");
                    var metadata = session.Advanced.GetMetadataFor(doc);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.Contains(DocumentFlags.HasRevisions.ToString(), flags);

                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("foo/bar");
                    Assert.Equal(3, revisions.Count);
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<User>("foo/bar");
                    doc.Age = 33;
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<User>("foo/bar");
                    var metadata = session.Advanced.GetMetadataFor(doc);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.Contains(DocumentFlags.HasRevisions.ToString(), flags);

                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("foo/bar");
                    Assert.Equal(3, revisions.Count);
                }
            }
        }

        [Fact]
        public void LocalIsLongerThanRemote()
        {
            var dbIds = new List<string> { new string('1', 22), new string('2', 22), new string('3', 22) };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 95, NodeTag = 2 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2, NodeTag = 1 },
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10, NodeTag = 0 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 75, NodeTag = 0 },
            };

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote.SerializeVector(), local.SerializeVector()));
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

                Index(x => x.Name, FieldIndexing.Search);

                Analyze(x => x.Name, typeof(RavenStandardAnalyzer).FullName);

            }
        }
    }
}

