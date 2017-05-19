using System.Threading.Tasks;
using FastTests.Server.Documents.Versioning;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Client.Server;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationWithVersioning : ReplicationTestsBase
    {
        [Fact(Skip = "http://issues.hibernatingrhinos.com/issue/RavenDB-6555")]
        public async Task CanReplicateVersions()
        {
            var company = new Company {Name = "Company Name"};
            var company2 = new Company {Name = "Company Name2"};

            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, master.Database);
                //await VersioningHelper.SetupVersioning(Server.ServerStore, slave.Database);

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
                Assert.Equal(2, WaitForValue(() => GetRevisions(slave, "foo/bar").Count, 2));
            }
        }

        [Fact(Skip = "http://issues.hibernatingrhinos.com/issue/RavenDB-6555")]
        public async Task CreateVersionsAndReplicateThemAll()
        {
            var company = new Company {Name = "Company Name"};
            var company2 = new Company {Name = "Company Name2"};
            var company3 = new Company {Name = "Company Name3"};
            var company4 = new Company {Name = "Company Name4"};

            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, master.Database);
                //await VersioningHelper.SetupVersioning(Server.ServerStore, slave.Database);

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
                Assert.Equal(4, WaitForValue(() => GetRevisions(slave, "foo/bar").Count, 4));
            }
        }

        [Fact(Skip = "http://issues.hibernatingrhinos.com/issue/RavenDB-6555")]
        public async Task ReplicateVersionsIgnoringConflicts()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await GenerateConflict(storeA, storeB);

                Assert.Equal(2, WaitUntilHasConflict(storeA, "foo/bar").Results.Length);
                Assert.Equal(2, WaitUntilHasConflict(storeB, "foo/bar").Results.Length);

                Assert.Equal(2, WaitForValue(() => GetRevisions(storeA, "foo/bar").Count, 2));
                Assert.Equal(2, WaitForValue(() => GetRevisions(storeB, "foo/bar").Count, 2));
            }
        }

        [Fact(Skip = "http://issues.hibernatingrhinos.com/issue/RavenDB-6555")]
        public async Task CreateConflictAndResolveItIncreaseTheVersion()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await GenerateConflict(storeA, storeB);

                Assert.Equal(2, WaitUntilHasConflict(storeA, "foo/bar").Results.Length);
                Assert.Equal(2, WaitUntilHasConflict(storeB, "foo/bar").Results.Length);

                Assert.Equal(2, WaitForValue(() => GetRevisions(storeA, "foo/bar").Count, 2));
                Assert.Equal(2, WaitForValue(() => GetRevisions(storeB, "foo/bar").Count, 2));

                var config = new ConflictSolver
                {
                    ResolveToLatest = true
                };

                await SetupReplicationAsync(storeA, config, storeB);

                Assert.True(WaitForDocument(storeA, "foo/bar"));
                Assert.True(WaitForDocument(storeB, "foo/bar"));

                Assert.Equal(3, WaitForValue(() => GetRevisions(storeA, "foo/bar").Count, 3));
                Assert.Equal(3, WaitForValue(() => GetRevisions(storeB, "foo/bar").Count, 3));
            }
        }


        private async Task GenerateConflict(DocumentStore storeA, DocumentStore storeB)
        {
            var user = new User { Name = "Name" };
            var user2 = new User { Name = "Name2" };
            
            await VersioningHelper.SetupVersioning(Server.ServerStore, storeA.Database);
            //await VersioningHelper.SetupVersioning(Server.ServerStore, storeB.Database);

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

        [Fact(Skip = "http://issues.hibernatingrhinos.com/issue/RavenDB-6555")]
        public async Task UpdateTheSameRevisoinWhenGettingExistingRevision()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            using (var storeC = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, storeA.Database);
                await VersioningHelper.SetupVersioning(Server.ServerStore, storeB.Database);
                await VersioningHelper.SetupVersioning(Server.ServerStore, storeC.Database);

                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.Equal(1, WaitForValue(() => GetRevisions(storeA, "users/1").Count, 1));
                Assert.Equal(1, WaitForValue(() => GetRevisions(storeB, "users/1").Count, 1));
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

                Assert.Equal(1, WaitForValue(() => GetRevisions(storeC, "users/1").Count, 1));
            }
        }
    }
}