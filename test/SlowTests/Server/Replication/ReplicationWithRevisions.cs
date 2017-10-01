using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Revisions;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationWithRevisions : ReplicationTestBase
    {
        [Fact]
        public async Task CanReplicateRevisions()
        {
            var company = new Company {Name = "Company Name"};
            var company2 = new Company {Name = "Company Name2"};

            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, master.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, slave.Database);
                await SetupReplicationAsync(master, slave);

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company2, "foo/bar");
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument(slave, "foo/bar"));
                Assert.Equal(2, WaitForValue(() => slave.Commands().GetRevisionsFor("foo/bar").Count, 2));
            }
        }

        [Fact]
        public async Task CreateRevisionsAndReplicateThemAll()
        {
            var company = new Company {Name = "Company Name"};
            var company2 = new Company {Name = "Company Name2"};
            var company3 = new Company {Name = "Company Name3"};
            var company4 = new Company {Name = "Company Name4"};

            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, master.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, slave.Database);

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company2, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company3, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company4, "foo/bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(master, slave);

                Assert.True(WaitForDocument(slave, "foo/bar"));
                Assert.Equal(4, WaitForValue(() => slave.Commands().GetRevisionsFor("foo/bar").Count, 4));
            }
        }

        public async Task ReplicateRevisionsIgnoringConflicts()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await GenerateConflict(storeA, storeB);

                Assert.Equal(2, WaitUntilHasConflict(storeA, "foo/bar").Length);
                Assert.Equal(2, WaitUntilHasConflict(storeB, "foo/bar").Length);

                Assert.Equal(2, WaitForValue(() => storeA.Commands().GetRevisionsFor("foo/bar").Count, 2));
                Assert.Equal(2, WaitForValue(() => storeB.Commands().GetRevisionsFor("foo/bar").Count, 2));
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CreateConflictAndResolveItIncreaseTheRevisions(bool configureVersioning)
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await GenerateConflict(storeA, storeB, configureVersioning);

                Assert.Equal(2, WaitUntilHasConflict(storeA, "foo/bar").Length);
                Assert.Equal(2, WaitUntilHasConflict(storeB, "foo/bar").Length);

                if (configureVersioning)
                {
                    Assert.Equal(2, WaitForValue(() => storeA.Commands().GetRevisionsFor("foo/bar").Count, 2));
                    Assert.Equal(2, WaitForValue(() => storeB.Commands().GetRevisionsFor("foo/bar").Count, 2));
                }

                var config = new ConflictSolver
                {
                    ResolveToLatest = true
                };

                await SetupReplicationAsync(storeA, config, storeB);

                Assert.True(WaitForDocument(storeA, "foo/bar"));
                Assert.True(WaitForDocument(storeB, "foo/bar"));

                Assert.Equal(3, WaitForValue(() => storeA.Commands().GetRevisionsFor("foo/bar").Count, 3));
                Assert.Equal(3, WaitForValue(() => storeB.Commands().GetRevisionsFor("foo/bar").Count, 3));
            }
        }


        private async Task GenerateConflict(DocumentStore storeA, DocumentStore storeB, bool configureVersioning = true)
        {
            var user = new User { Name = "Name" };
            var user2 = new User { Name = "Name2" };

            if (configureVersioning)
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeA.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeB.Database);
            }

            using (var session = storeA.OpenAsyncSession())
            {
                await session.StoreAsync(user, "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = storeB.OpenAsyncSession())
            {
                await session.StoreAsync(user2, "foo/bar");
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(storeA, storeB);
            await SetupReplicationAsync(storeB, storeA);
        }

        [Fact]
        public async Task UpdateTheSameRevisionWhenGettingExistingRevision()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            using (var storeC = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeA.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeB.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeC.Database);

                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.Equal(1, WaitForValue(() => storeA.Commands().GetRevisionsFor("users/1").Count, 1));
                Assert.Equal(1, WaitForValue(() => storeB.Commands().GetRevisionsFor("users/1").Count, 1));
                Assert.True(WaitForDocument(storeB, "users/1"));

                await SetupReplicationAsync(storeA, storeC);
                await SetupReplicationAsync(storeB, storeC);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Marker" }, "marker");
                    await session.SaveChangesAsync();
                }
                Assert.True(WaitForDocument(storeC, "marker"));
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Marker" }, "marker");
                    await session.SaveChangesAsync();
                }
                Assert.True(WaitForDocument(storeB, "marker"));

                Assert.Equal(1, WaitForValue(() => storeC.Commands().GetRevisionsFor("users/1").Count, 1));
            }
        }
    }
}
