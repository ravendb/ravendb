using System.Threading.Tasks;
using FastTests.Server.Documents.Versioning;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationWithVersioning : ReplicationTestsBase
    {
        [Fact]
        public async Task CanReplicateVersions()
        {
            var company = new Company {Name = "Company Name"};
            var company2 = new Company {Name = "Company Name2"};

            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(master);
                await VersioningHelper.SetupVersioning(slave);

                SetupReplication(master, slave);

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

        [Fact]
        public async Task CreateVersionsAndReplicateThemAll()
        {
            var company = new Company {Name = "Company Name"};
            var company2 = new Company {Name = "Company Name2"};
            var company3 = new Company {Name = "Company Name3"};
            var company4 = new Company {Name = "Company Name4"};

            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(master);
                await VersioningHelper.SetupVersioning(slave);

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

                SetupReplication(master, slave);

                Assert.True(WaitForDocument(slave, "foo/bar"));
                Assert.Equal(4, WaitForValue(() => GetRevisions(slave, "foo/bar").Count, 4));
            }
        }

        [Fact]
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

        [Fact]
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

                SetupReplication(storeA, new ReplicationDocument
                {
                    DocumentConflictResolution = StraightforwardConflictResolution.ResolveToLatest
                }, storeB);

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
            
            await VersioningHelper.SetupVersioning(storeA);
            await VersioningHelper.SetupVersioning(storeB);

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

            SetupReplication(storeA, storeB);
            SetupReplication(storeB, storeA);
        }

        [Fact]
        public async Task UpdateTheSameRevisoinWhenGettingExistingRevision()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            using (var storeC = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(storeA);
                await VersioningHelper.SetupVersioning(storeB);
                await VersioningHelper.SetupVersioning(storeC);

                SetupReplication(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.Equal(1, WaitForValue(() => GetRevisions(storeA, "users/1").Count, 1));
                Assert.Equal(1, WaitForValue(() => GetRevisions(storeB, "users/1").Count, 1));
                Assert.True(WaitForDocument(storeB, "users/1"));

                SetupReplication(storeA, storeC);
                SetupReplication(storeB, storeC);

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