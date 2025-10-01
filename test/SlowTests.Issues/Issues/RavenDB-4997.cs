using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4997 : ReplicationTestBase
    {
        public RavenDB_4997(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Load_of_conflicted_document_with_tombstone_should_result_in_error(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Load_of_conflicted_document_with_another_document_should_result_in_error(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Delete_of_conflicted_document_should_resolve_conflict(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
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

                var conflicts = storeA.Commands().GetConflictsFor("users/1");
                Assert.Equal(0, conflicts.Length);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Load_of_several_conflicted_document_should_result_in_error(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            using (var storeC = GetDocumentStore(options))
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
