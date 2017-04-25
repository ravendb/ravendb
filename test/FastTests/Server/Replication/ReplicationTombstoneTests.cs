using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationTombstoneTests : ReplicationTestsBase
    {
        [Fact]
        public async Task Tombstones_replication_should_delete_document_at_multiple_destinations_fan()
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            var dbName3 = "FooBar-3";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            using (var store3 = GetDocumentStore(dbSuffixIdentifier: dbName3))
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

        [Fact]
        public async Task Tombstones_replication_should_delete_document_at_multiple_destinations_chain()
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

        [Fact]
        public async Task Tombstone_should_replicate_in_master_master_cycle()
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

        [Fact]
        public async Task Replication_of_document_should_delete_existing_tombstone_at_destination()
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
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
            }
        }

        [Fact]
        public async Task CreateConflictAndResolveItWithTombstone()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {

                using (var sessoin = store1.OpenSession())
                {
                    sessoin.Store(new User { Name = "foo" }, "foo/bar");
                    sessoin.SaveChanges();
                }

                using (var sessoin = store2.OpenSession())
                {
                    sessoin.Store(new User { Name = "bar" }, "foo/bar");
                    sessoin.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Results.Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);

                using (var sessoin = store1.OpenSession())
                {
                    sessoin.Delete("foo/bar");
                    sessoin.SaveChanges();
                }

                Assert.Equal(1, WaitUntilHasTombstones(store1).Count);
                Assert.Equal(1, WaitUntilHasTombstones(store2).Count);
            }
        }
    }
}
