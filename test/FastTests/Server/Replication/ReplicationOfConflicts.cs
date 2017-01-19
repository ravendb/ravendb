using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationOfConflicts : ReplicationBasicTests
    {

        [Fact]
        public void ReplicateAConflictThenResolveIt()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();

            using (var session = store1.OpenSession())
            {
                session.Store(new User {Name = "Karmel"},"foo/bar");
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Store(new User { Name = "Oren" }, "foo/bar");
                session.SaveChanges();
            }

            SetupReplication(store1,store2);

            var conflicts = WaitUntilHasConflict(store2, "foo/bar");
            Assert.Equal(2, conflicts["foo/bar"].Count);

            SetupReplication(store2, store1);

            conflicts = WaitUntilHasConflict(store1, "foo/bar");
            Assert.Equal(2, conflicts["foo/bar"].Count);

            // adding new document, resolve the conflict
            using (var session = store2.OpenSession())
            {
                session.Store(new User { Name = "Resolved" }, "foo/bar");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(store1, "foo/bar"));

            Assert.Empty(GetConflicts(store1, "foo/bar"));
            Assert.Empty(GetConflicts(store2, "foo/bar"));
        }

        [Fact]
        public void ReplicateAConflictOnThreeDBsAndResolve()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();
            var store3 = GetDocumentStore();

            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "foo/bar");
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Store(new User { Name = "Oren" }, "foo/bar");
                session.SaveChanges();
            }

            SetupReplication(store1, store2, store3);

            var conflicts = WaitUntilHasConflict(store2, "foo/bar");
            Assert.Equal(2, conflicts["foo/bar"].Count);

            SetupReplication(store2, store1);

            conflicts = WaitUntilHasConflict(store1, "foo/bar");
            Assert.Equal(2, conflicts["foo/bar"].Count);

            conflicts = WaitUntilHasConflict(store3, "foo/bar");
            Assert.Equal(2, conflicts["foo/bar"].Count);

            using (var session = store2.OpenSession())
            {
                session.Store(new User { Name = "Resolved" }, "foo/bar");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(store1, "foo/bar"));
            Assert.True(WaitForDocument(store3, "foo/bar"));
        }

        [Fact]
        public void ReplicateTombstoneConflict()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();

            SetupReplication(store1, store2);

            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "foo/bar");
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(store2,"foo/bar"));

            using (var session = store2.OpenSession())
            {
                session.Delete("foo/bar");
                session.SaveChanges();
            }
            Assert.True(WaitForDocumentDeletion(store2,"foo/bar"));

            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Oren" }, "foo/bar");
                session.SaveChanges();
            }

            var conflicts = WaitUntilHasConflict(store2, "foo/bar");
            Assert.Equal(2, conflicts["foo/bar"].Count);

            SetupReplication(store2, store1);
         

            conflicts = WaitUntilHasConflict(store1, "foo/bar");
            Assert.Equal(2, conflicts["foo/bar"].Count);
        }
    }
}
