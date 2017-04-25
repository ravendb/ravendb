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
        public async Task ReplicateAConflictThenResolveIt()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {

                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new User {Name = "Oren"}, "foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);

                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);

                await SetupReplicationAsync(store2, store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Results.Length);

                // adding new document, resolve the conflict
                using (var session = store2.OpenSession())
                {
                    session.Store(new User {Name = "Resolved"}, "foo/bar");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(store1, "foo/bar"));

                Assert.Empty(GetConflicts(store1, "foo/bar").Results);
                Assert.Empty(GetConflicts(store2, "foo/bar").Results);
            }
        }  

        [Fact]
        public async Task CanManuallyResolveConflict_with_tombstone()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                await SetupReplicationAsync(master, slave);

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(slave, "users/1"));

                using (var session = slave.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.Store(new User
                    {
                        Name = "Karmeli-2"
                    }, "final-marker");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(slave, "final-marker"));

                using (var session = slave.OpenSession())
                {
                    try
                    {
                        session.Load<User>("users/1");                        
                    }
                    catch (DocumentConflictException e)
                    {
                        Assert.Equal(e.DocId, "users/1");
                        Assert.NotEqual(e.LargestEtag, 0);
                        slave.Commands().Delete("users/1", null); //resolve conflict to the one with tombstone
                    }

                    //after resolving the conflict, should not throw
                    Assert.Null(session.Load<User>("users/1"));
                }
            }
        }

        [Fact]
        public async Task ReplicateAConflictOnThreeDBsAndResolve()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            using (var store3 = GetDocumentStore())
            {

                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new User {Name = "Oren"}, "foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2, store3);

                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);

                await SetupReplicationAsync(store2, store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Results.Length);
                Assert.Equal(2, WaitUntilHasConflict(store3, "foo/bar").Results.Length);

                using (var session = store2.OpenSession())
                {
                    session.Store(new User {Name = "Resolved"}, "foo/bar");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(store1, "foo/bar"));
                Assert.True(WaitForDocument(store3, "foo/bar"));
            }
        }

        [Fact]
        public async Task ReplicateTombstoneConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(store2, "foo/bar"));

                using (var session = store2.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocumentDeletion(store2, "foo/bar"));

                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Oren"}, "foo/bar");
                    session.SaveChanges();
                }

                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);

                await SetupReplicationAsync(store2, store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Results.Length);
            }
        }
    }
}
