using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Replication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6415 : ReplicationTestBase
    {
        public RavenDB_6415(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task PUT_of_conflicted_document_with_outdated_etag_throws_concurrency_exception(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "John Doe" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Jane Doe" }, "users/1");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                WaitUntilHasConflict(storeA, "users/1");

                long maxConflictEtag;
                using (var session = storeA.OpenSession())
                {
                    var ex = Assert.Throws<DocumentConflictException>(() => session.Load<User>("users/1"));
                    maxConflictEtag = ex.LargestEtag;
                }
                //should throw concurrency exception because we use lower etag then max etag of existing conflicts
                using (var session = storeA.OpenSession())
                {
                    var db = await GetDocumentDatabaseInstanceForAsync(storeA, options.DatabaseMode, "users/1");
                    var cv = new ChangeVectorEntry[1];
                    cv[0] = new ChangeVectorEntry
                    {
                        DbId = db.DbBase64Id,
                        Etag = maxConflictEtag - 1
                    };
                    session.Store(new User { Name = "James Doe" }, cv.SerializeVector(), "users/1");
                    Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                }

                //now this should _not_ throw, since we do not specify expected conflict etag, so...
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "James Doe" }, "users/1");
                    session.SaveChanges();
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DELETE_of_conflicted_document_with_outdated_etag_throws_concurrency_exception(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "John Doe" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Jane Doe" }, "users/1");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                WaitUntilHasConflict(storeA, "users/1");

                long maxConflictEtag;
                using (var session = storeA.OpenSession())
                {
                    var ex = Assert.Throws<DocumentConflictException>(() => session.Load<User>("users/1"));
                    maxConflictEtag = ex.LargestEtag;
                }

                //should throw concurrency exception because we use lower etag then max etag of existing conflicts
                using (var session = storeA.OpenSession())
                {
                    var db = await GetDocumentDatabaseInstanceForAsync(storeA, options.DatabaseMode, "users/1");
                    var cv = new ChangeVectorEntry[1];
                    cv[0] = new ChangeVectorEntry
                    {
                        DbId = db.DbBase64Id,
                        Etag = maxConflictEtag - 1
                    };
                    session.Delete("users/1", cv.SerializeVector());
                    Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                }

                //now this should _not_ throw, since we do not specify expected conflict etag, so...
                using (var session = storeA.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
            }
        }
    }
}
