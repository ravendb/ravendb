using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationTombstoneTestsSlow : ReplicationTestsBase
    {

        [Fact]
        public async Task Tombstones_replication_should_delete_document_at_destination()
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

                Assert.False(WaitForDocument(store2, "foo/bar", 1000));
            }
        }

        [Fact]
        public async Task Tombstone_should_replicate_in_master_master()
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
                await SetupReplicationAsync(store2, store1);

                Assert.True(WaitForDocument(store2, "foo/bar"));

                using (var s2 = store1.OpenSession())
                {
                    s2.Delete("foo/bar");
                    s2.SaveChanges();
                }

                var tombstoneIDs = WaitUntilHasTombstones(store1);
                Assert.Equal(1, tombstoneIDs.Count);
                Assert.Contains("foo/bar", tombstoneIDs);

                var timeout = 1000 * Server.ServerStore.DatabasesLandlord.LastRecentlyUsed.Count;
                Assert.False(WaitForDocument(store1, "foo/bar", timeout));
            }
        }


        [Fact]
        public async Task Two_tombstones_should_replicate_in_master_master()
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