using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationCleanTombstones : ReplicationTestsBase
    {
        [Fact]
        public void DontCleanTombstones()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();

            var storage1 = GetDocumentDatabaseInstanceFor(store1).Result;

            using (var session = store1.OpenSession())
            {
                session.Store(new User {Name = "Karmel"},"foo/bar");
                session.SaveChanges();
            }

            SetupReplication(store1,store2);
            using (var session = store1.OpenSession())
            {
                session.Delete("foo/bar");
                session.SaveChanges();
                storage1.DocumentTombstoneCleaner.ExecuteCleanup(null);
            }
            Assert.Equal(1,WaitUntilHasTombstones(store1).Count);
        }

        [Fact]
        public void CleanTombstones()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();

            var storage1 = GetDocumentDatabaseInstanceFor(store1).Result;

            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "foo/bar");
                session.SaveChanges();
            }

            SetupReplication(store1, store2);
            Assert.True(WaitForDocument(store2, "foo/bar"));

            using (var session = store1.OpenSession())
            {
                session.Delete("foo/bar");
                session.SaveChanges();
            }

            Assert.Equal(1, WaitUntilHasTombstones(store2).Count);
            Assert.Equal(4, WaitForValue(() => storage1.ReplicationLoader.MinimalEtagForReplication, 4));
            storage1.DocumentTombstoneCleaner.ExecuteCleanup(null);
            Assert.Equal(0, WaitUntilHasTombstones(store1,0).Count);
        }
    }
}
