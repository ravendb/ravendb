using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4997 : ReplicationTestsBase
    {
        private class User
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task Load_of_conflicted_document_with_tombstone_should_result_in_error()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "foo" }, "users/1");
                    session.Store(new User { Name = "bar-foo" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "bar" }, "users/1");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeB, storeA);

                WaitUntilHasConflict(storeA, "users/1");

                using (var session = storeA.OpenSession())
                {

                    var e = Assert.Throws<DocumentConflictException>(() => session.Load<User>("users/1"));
                    Assert.Equal("users/1", e.DocId);

                    session.Load<User>("users/2"); //this should not throw
                }
            }
        }

        [Fact]
        public async Task Load_of_conflicted_document_with_another_document_should_result_in_error()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "foo" }, "users/1");
                    session.Store(new User { Name = "bar-foo" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "bar" }, "users/1");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeB, storeA);

                WaitUntilHasConflict(storeA, "users/1");

                using (var session = storeA.OpenSession())
                {

                    var e = Assert.Throws<DocumentConflictException>(() => session.Load<User>("users/1"));
                    Assert.Equal("users/1", e.DocId);

                    session.Load<User>("users/2"); //this should not throw
                }
            }
        }

        [Fact]
        public async Task Delete_of_conflicted_document_should_resolve_conflict()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "foo" }, "users/1");
                    session.Store(new User { Name = "bar-foo" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "bar" }, "users/1");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeB, storeA);

                WaitUntilHasConflict(storeA, "users/1");

                using (var session = storeA.OpenSession())
                {
                    session.Delete("users/1"); 
                    session.SaveChanges();
                }

                var conflicts = GetConflicts(storeA, "users/1");
                Assert.Equal(0,conflicts.Results.Length);
            }
        }

        [Fact]
        public async Task Load_of_several_conflicted_document_should_result_in_error()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            using (var storeC = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "foo" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "bar" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = storeC.OpenSession())
                {
                    session.Store(new User { Name = "foo-bar" }, "users/1");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeB, storeA);
                await SetupReplicationAsync(storeC, storeA);

                WaitUntilHasConflict(storeA, "users/1", 3);

                using (var session = storeA.OpenSession())
                {
                    var e = Assert.Throws<DocumentConflictException>(() => session.Load<User>("users/1"));
                    Assert.Equal("users/1", e.DocId);
                }
            }
        }
    }
}