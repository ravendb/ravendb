using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationCleanTombstones : ReplicationTestsBase
    {
        [Fact]
        public async Task CleanTombstones()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var storage1 = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.DefaultDatabase).Result;

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                Assert.True(WaitForDocument(store2, "foo/bar"));

                using (var session = store1.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }
                while (storage1.DocumentTombstoneCleaner.ExecuteCleanup() == false)
                {
                    Thread.Sleep(16);
                }
                Assert.Equal(1, WaitUntilHasTombstones(store2).Count);
                //Assert.Equal(4, WaitForValue(() => storage1.ReplicationLoader.MinimalEtagForReplication, 4));
                while (storage1.DocumentTombstoneCleaner.ExecuteCleanup() == false)
                {
                    Thread.Sleep(16);
                }
                Assert.Equal(0, WaitUntilHasTombstones(store1, 0).Count);
            }
        }
    }
}
