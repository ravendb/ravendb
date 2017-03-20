using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions;
using Raven.Tests.Core.Utils.Entities;
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

            Assert.Empty(GetConflicts(store1, "foo/bar").Results);
            Assert.Empty(GetConflicts(store2, "foo/bar").Results);
        }  

        [Fact]
        public void CanManuallyResolveConflict_with_tombstone()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                SetupReplication(master, slave);

                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var updated = WaitForDocument(slave, "users/1");
                Assert.True(updated);

                using (var session = slave.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmeli"
                    }, "users/1");

                    session.Store(new User()
                    {
                        Name = "Karmeli-2"
                    }, "final-marker");

                    session.SaveChanges();
                }

                var updated2 = WaitForDocument(slave, "final-marker");
                Assert.True(updated2);

                using (var session = slave.OpenSession())
                {
                    try
                    {
                        session.Load<User>("users/1");                        
                    }
                    catch (DocumentConflictException e)
                    {
                        Assert.Equal(e.DocId,"users/1");
                        Assert.NotEqual(e.LargestEtag, 0);
                        slave.Commands().Delete("users/1",null);//resolve conflict to the one with tombstone
                    }
                }

                using (var session = slave.OpenSession())
                {
                    //after resolving the conflict, should not throw
                    var doc = session.Load<User>("users/1");
                    Assert.Null(doc);
                }
            }
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
