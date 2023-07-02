using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationTombstoneTests : ReplicationTestBase
    {
        public ReplicationTombstoneTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DontReplicateTombstoneBack(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                string changeVector1;
                var databaseName = options.DatabaseMode == RavenDatabaseMode.Single ? store1.Database : await Sharding.GetShardDatabaseNameForDocAsync(store1, "users/1");
                var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

                using (var session = store1.OpenSession())
                {
                    var user = new User
                    {
                        Name = "John Dow",
                        Age = 30
                    };
                    session.Store(user, "users/1");
                    session.SaveChanges();
                    session.Delete(user);
                    session.SaveChanges();

                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        changeVector1 = documentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, "users/1").Tombstone.ChangeVector;
                    }
                }
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.True(WaitForDocumentDeletion(store2, "users/1"));
                await Task.Delay((int)(documentDatabase.ReplicationLoader.MinimalHeartbeatInterval * 2.5));

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var changeVector2 = documentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, "users/1").Tombstone.ChangeVector;
                    Assert.Equal(changeVector1, changeVector2);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RavenDB_12295(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    var user = new User
                    {
                        Name = "John Dow",
                        Age = 30
                    };
                    session.Store(user, "users/1");
                    session.SaveChanges();

                    session.Delete(user);
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                using (var session = store1.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    var user = new User
                    {
                        Name = "John Dow",
                        Age = 30
                    };
                    session.Store(user, "users/1");
                    session.SaveChanges();

                    await Task.Delay(2500);

                    var changeVector = session.Advanced.GetChangeVectorFor(user);

                    session.Delete(user.Id, changeVector);
                    session.SaveChanges();
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Tombstones_replication_should_delete_document_at_multiple_destinations_fan(Options options)
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            var dbName3 = "FooBar-3";
            using (var store1 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}"
            }))
            using (var store2 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}"
            }))
            using (var store3 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName3}"
            }))
            {
                await SetupReplicationAsync(store1, store2, store3);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "foo/bar"), store2.Identifier);
                Assert.True(WaitForDocument(store3, "foo/bar"), store3.Identifier);

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete("foo/bar");
                    s1.SaveChanges();
                }

                var tombstoneIDs = WaitUntilHasTombstones(store2);
                Assert.Equal(1, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);

                Assert.True(WaitForDocumentDeletion(store2, "foo/bar", 1000));

                tombstoneIDs = WaitUntilHasTombstones(store3);
                Assert.Equal(1, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);

                Assert.True(WaitForDocumentDeletion(store3, "foo/bar", 1000));
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Tombstones_replication_should_delete_document_at_multiple_destinations_chain(Options options)
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            var dbName3 = "FooBar-3";
            using (var store1 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}"
            }))
            using (var store2 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}"
            }))
            using (var store3 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName3}"
            }))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store3);

                Assert.True(WaitForDocument(store2, "foo/bar", 20000), store2.Identifier);
                Assert.True(WaitForDocument(store3, "foo/bar", 20000), store3.Identifier);

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete("foo/bar");
                    s1.SaveChanges();
                }

                var tombstoneIDs = WaitUntilHasTombstones(store2);
                Assert.Equal(1, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);

                Assert.True(WaitForDocumentDeletion(store2, "foo/bar", 1000));

                tombstoneIDs = WaitUntilHasTombstones(store3);
                Assert.Equal(1, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);

                Assert.True(WaitForDocumentDeletion(store3, "foo/bar", 1000));
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Tombstone_should_replicate_in_master_master_cycle(Options options)
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            var dbName3 = "FooBar-3";
            using (var store1 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}"
            }))
            using (var store2 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}"
            }))
            using (var store3 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName3}"
            }))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                using (var s2 = store1.OpenSession())
                {
                    s2.Store(new User(), "foo/bar2");
                    s2.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2, store3);
                await SetupReplicationAsync(store2, store1, store3);
                await SetupReplicationAsync(store3, store2, store1);

                Assert.True(WaitForDocument(store1, "foo/bar"));
                Assert.True(WaitForDocument(store2, "foo/bar"));
                Assert.True(WaitForDocument(store3, "foo/bar"));

                Assert.True(WaitForDocument(store1, "foo/bar2"));
                Assert.True(WaitForDocument(store2, "foo/bar2"));
                Assert.True(WaitForDocument(store3, "foo/bar2"));

                using (var s2 = store1.OpenSession())
                {
                    s2.Delete("foo/bar");
                    s2.SaveChanges();
                }

                using (var s3 = store1.OpenSession())
                {
                    s3.Delete("foo/bar2");
                    s3.SaveChanges();
                }

                var tombstoneIDs = WaitUntilHasTombstones(store1, 2);
                Assert.Equal(2, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);
                Assert.Contains("foo/bar2", tombstoneIDs);

                tombstoneIDs = WaitUntilHasTombstones(store2, 2);
                Assert.Equal(2, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);
                Assert.Contains("foo/bar2", tombstoneIDs);

                tombstoneIDs = WaitUntilHasTombstones(store3, 2);
                Assert.Equal(2, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);
                Assert.Contains("foo/bar2", tombstoneIDs);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Replication_of_document_should_delete_existing_tombstone_at_destination(Options options)
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}"
            }))
            using (var store2 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}"
            }))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);

                Assert.True(WaitForDocument(store2, "foo/bar"));

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete("foo/bar");
                    s1.SaveChanges();
                }

                var tombstoneIDs = WaitUntilHasTombstones(store2);
                Assert.Equal(1, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);

                var stats = await GetDatabaseStatisticsAsync(store2);
                Assert.Equal(1, stats.CountOfTombstones);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                //first wait until everything is replicated
                Assert.True(WaitForDocument(store1, "foo/bar"));
                Assert.True(WaitForDocument(store2, "foo/bar"));

                //then verify that tombstone is deleted
                var tombstonesAtStore2 = GetTombstones(store2);
                Assert.Empty(tombstonesAtStore2);

                stats = await GetDatabaseStatisticsAsync(store2);
                Assert.Equal(0, stats.CountOfTombstones);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CreateConflictAndResolveItWithTombstone(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var store1 = GetDocumentStore(options: options))
            using (var store2 = GetDocumentStore(options: options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "foo" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "bar" }, "foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Length);

                using (var session = store1.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                Assert.Equal(1, WaitUntilHasTombstones(store1).Count);
                Assert.Equal(1, WaitUntilHasTombstones(store2).Count);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Tombstones_replication_should_delete_document_at_destination(Options options)
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}"
            }))
            using (var store2 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}"
            }))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);

                Assert.True(WaitForDocument(store2, "foo/bar"));

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete("foo/bar");
                    s1.SaveChanges();
                }

                var tombstoneIDs = WaitUntilHasTombstones(store2);
                Assert.Equal(1, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);

                Assert.False(WaitForDocument(store2, "foo/bar", 1000));
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Tombstone_should_replicate_in_master_master(Options options)
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}"
            }))
            using (var store2 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}"
            }))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.True(WaitForDocument(store2, "foo/bar"));

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete("foo/bar");
                    s1.SaveChanges();
                }

                var tombstoneIDs = WaitUntilHasTombstones(store1);
                Assert.Equal(1, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);

                await EnsureReplicatingAsync(store1, store2);

                Assert.False(WaitForDocument(store1, "foo/bar"));
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Two_tombstones_should_replicate_in_master_master(Options options)
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName1}"
            }))
            using (var store2 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_{dbName2}"
            }))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar2");
                    s1.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                var timeout = 1000 * Server.ServerStore.DatabasesLandlord.LastRecentlyUsed.Count;
                Assert.True(WaitForDocument(store2, "foo/bar", timeout));
                Assert.True(WaitForDocument(store2, "foo/bar2", timeout));

                using (var s2 = store1.OpenSession())
                {
                    s2.Delete("foo/bar");
                    s2.Delete("foo/bar2");
                    s2.SaveChanges();
                }

                var tombstoneIDs = WaitUntilHasTombstones(store1);
                Assert.Equal(2, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);

                Assert.False(WaitForDocument(store1, "foo/bar", 100));
            }
        }
    }
}
