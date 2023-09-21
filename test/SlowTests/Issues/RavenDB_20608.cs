using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20608 : ReplicationTestBase
    {
        public RavenDB_20608(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicationShouldNotCauseConflictAfterResolve(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            using (var storeC = GetDocumentStore(options))
            {
                // create conflict of identical document
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, "users/shiran");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, "users/shiran");
                    session.SaveChanges();
                }

                // set replication from A, B to C
                await SetupReplicationAsync(storeA, storeC);
                await EnsureReplicatingAsync(storeA, storeC);

                await SetupReplicationAsync(storeB, storeC);
                await EnsureReplicatingAsync(storeB, storeC);

                // resolve a conflict of identical documents should not create conflicted revisions
                await AssertRevisionsCountAsync(storeC, 0);

                // update the document on B
                // this should cause a conflict on C
                using (var session = storeB.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 30;
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeB, storeC);
                await AssertRevisionsCountAsync(storeC, 3);

                // update the document on A 
                using (var session = storeA.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 30;
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeA, storeC);

                // set replication between A, B 
                // from that point on any update of the document from A or B should not cause a conflict on C
                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                await EnsureReplicatingAsync(storeB, storeA);

                // update the document again to ensure additional revision was not created in any store
                using (var session = storeA.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 50;
                    session.SaveChanges();
                }

                await AssertRevisionsCountAsync(storeA, 0);
                await AssertRevisionsCountAsync(storeB, 0);
                await AssertRevisionsCountAsync(storeC, 3);
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicationShouldNotCauseConflictAfterResolve2(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            using (var storeC = GetDocumentStore(options))
            using (var storeD = GetDocumentStore(options))
            {
                // create conflict
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, "users/shiran");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Shiran2" }, "users/shiran");
                    session.SaveChanges();
                }

                // set replication from A, B to C
                await SetupReplicationAsync(storeA, storeC);
                await EnsureReplicatingAsync(storeA, storeC);

                await SetupReplicationAsync(storeB, storeC);
                await EnsureReplicatingAsync(storeB, storeC);

                // resolve to latest should create conflicted revisions
                await AssertRevisionsCountAsync(storeC, 3);

                // create the document on D
                // this will cause a conflict in replication with the resolved document that exists on C 
                using (var session = storeD.OpenSession())
                {
                    session.Store(new User { Name = "Shiran3" }, "users/shiran");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeD, storeC);
                await EnsureReplicatingAsync(storeD, storeC);

                // 3 conflicted revisions (from A, B, D), 1 resolved revision (A, B), 1 resolved revision (A, B, D)
                await AssertRevisionsCountAsync(storeC, 5);

                // set replication between A, B, D
                // from that point on, any update of the document from A, B or D should not cause a conflict on C
                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                await EnsureReplicatingAsync(storeB, storeA);
               
                await SetupReplicationAsync(storeD, storeA, storeB);
                await EnsureReplicatingAsync(storeD, storeA);
                await EnsureReplicatingAsync(storeD, storeB);

                await SetupReplicationAsync(storeA, storeD);
                await EnsureReplicatingAsync(storeA, storeD);

                await SetupReplicationAsync(storeB, storeD);
                await EnsureReplicatingAsync(storeB, storeD);
                await EnsureReplicatingAsync(storeA, storeB);

                await AssertRevisionsCountAsync(storeC, 5);

                using (var session = storeA.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 30;
                    session.SaveChanges();
                }
              
                Assert.True(WaitForDocument<User>(storeC, "users/shiran", u => u.Age == 30));

                await AssertRevisionsCountAsync(storeC, 5);
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions | RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicationWithAttachmentsShouldNotCauseConflictAfterResolve(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            using (var storeC = GetDocumentStore(options))
            {
                // create conflict of identical document with same attachment
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, "users/shiran");
                    session.Advanced.Attachments.Store("users/shiran", "foo/bar", profileStream, "image/png");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, "users/shiran");
                    session.Advanced.Attachments.Store("users/shiran", "foo/bar", profileStream, "image/png");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeC);
                await EnsureReplicatingAsync(storeA, storeC);

                await SetupReplicationAsync(storeB, storeC);
                await EnsureReplicatingAsync(storeB, storeC);

                using (var session = storeB.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 30;
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeB, storeC);

                using (var session = storeA.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 30;
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeA, storeC);

                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                await EnsureReplicatingAsync(storeB, storeA);

                using (var session = storeA.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 50;
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument<User>(storeC, "users/shiran", u => u.Age == 50));

                await AssertRevisionsCountAsync(storeA, 0);
                await AssertRevisionsCountAsync(storeB, 0);
                await AssertRevisionsCountAsync(storeC, 3);
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions | RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicationWithAttachmentsShouldNotCauseConflictAfterResolve2(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            using (var storeC = GetDocumentStore(options))
            {
                // create conflict of identical document with different attachments
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, "users/shiran");
                    session.Advanced.Attachments.Store("users/shiran", "foo/bar", profileStream, "image/png");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 4, 5, 6 }))
                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, "users/shiran");
                    session.Advanced.Attachments.Store("users/shiran", "foo/bar", profileStream, "image/png");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeC);
                await EnsureReplicatingAsync(storeA, storeC);

                await SetupReplicationAsync(storeB, storeC);
                await EnsureReplicatingAsync(storeB, storeC);

                using (var session = storeB.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 30;
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeB, storeC);

                using (var session = storeA.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 30;
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeA, storeC);

                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                await EnsureReplicatingAsync(storeB, storeA);

                await AssertRevisionsCountAsync(storeA, 3);
                await AssertRevisionsCountAsync(storeB, 3);
                await AssertRevisionsCountAsync(storeC, 7);

                using (var session = storeA.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 30;
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(storeC, "users/shiran", u => u.Age == 30));

                await AssertRevisionsCountAsync(storeC, 7);
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions | RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task GenerateNewChangeVectorInReplicationWithAttachmentConflict(Options options)
        {
            // unlike the previous tests, this one should create a new change vector
            // because we recreated the attachment reference locally in the incoming replication (RavenDB-19421)
            options.ModifyDatabaseRecord = record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = 1.ToString();
            };

            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            using (var storeC = GetDocumentStore(options))
            {
                // create conflict of identical document with different attachments
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, "users/shiran");
                    session.Advanced.Attachments.Store("users/shiran", "foo/bar", profileStream, "image/png");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 4, 5, 6 }))
                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, "users/shiran");
                    session.Advanced.Attachments.Store("users/shiran", "foo/bar", profileStream, "image/png");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeC);
                await EnsureReplicatingAsync(storeA, storeC);

                await SetupReplicationAsync(storeB, storeC);
                await EnsureReplicatingAsync(storeB, storeC);

                using (var session = storeA.OpenSession())
                {
                    session.Advanced.Attachments.Delete("users/shiran", "foo/bar");
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeB, storeC);

                using (var session = storeB.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 30;
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeA, storeC);

                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                await EnsureReplicatingAsync(storeB, storeA);

                await AssertRevisionsCountAsync(storeA, 3);
                await AssertRevisionsCountAsync(storeB, 3);
                await AssertRevisionsCountAsync(storeC, 8);

                using (var session = storeA.OpenSession())
                {
                    var user = session.Load<User>("users/shiran");
                    user.Age = 40;
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(storeC, "users/shiran", u => u.Age == 40));

                await AssertRevisionsCountAsync(storeC, 11);
            }
        }

        private async Task AssertRevisionsCountAsync(IDocumentStore store, int expectedNumberOfRevisions)
        {
            using (var session = store.OpenAsyncSession())
            {
                var revisionsCount = await session.Advanced.Revisions.GetCountForAsync("users/shiran");
                Assert.Equal(expectedNumberOfRevisions, revisionsCount);
            }
        }
    }
}
