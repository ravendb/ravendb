using System.Threading.Tasks;
using FastTests.Server.Documents.Versioning;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Server.Config.Categories;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationWithVersioning : ReplicationTestsBase
    {
        [Fact]
        public async Task CanReplicateVersions()
        {
            var company = new Company { Name = "Company Name" };
            var company2 = new Company { Name = "Company Name2" };

            var master = GetDocumentStore();
            var slave = GetDocumentStore();
            
            await VersioningHelper.SetupVersioning(master);
            await VersioningHelper.SetupVersioning(slave);

            SetupReplication(master, slave);

            using (var session = master.OpenAsyncSession())
            {
                await session.StoreAsync(company,"foo/bar");
                await session.SaveChangesAsync();                
            }

            using (var session = master.OpenAsyncSession())
            {
                await session.StoreAsync(company2, "foo/bar");
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(slave, "foo/bar"));
            Assert.Equal(2,WaitForValue(() => GetRevisions(slave, "foo/bar").Count, 2));

        }

        [Fact]
        public async Task CreateVersionsAndReplicateThemAll()
        {
            var company = new Company { Name = "Company Name" };
            var company2 = new Company { Name = "Company Name2" };
            var company3 = new Company { Name = "Company Name3" };
            var company4 = new Company { Name = "Company Name4" };

            var master = GetDocumentStore();
            var slave = GetDocumentStore();

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

        [Fact]
        public async Task ReplicateVersionsIgnoringConflicts()
        {
            var master = GetDocumentStore();
            var slave = GetDocumentStore();

            await GenerateConflict(master, slave);

            Assert.Equal(2, WaitUntilHasConflict(slave, "foo/bar")["foo/bar"].Count);
            Assert.Equal(2, WaitUntilHasConflict(slave, "foo/bar")["foo/bar"].Count);

            Assert.Equal(2, WaitForValue(() => GetRevisions(slave, "foo/bar").Count, 2));
            Assert.Equal(2, WaitForValue(() => GetRevisions(master, "foo/bar").Count, 2));
        }

        [Fact]
        public async Task CreateConflictAndResolveItIncreaseTheVersion()
        {
            
            var master = GetDocumentStore();
            var slave = GetDocumentStore();

            await GenerateConflict(master, slave);
            
            Assert.Equal(2, WaitUntilHasConflict(slave, "foo/bar")["foo/bar"].Count);
            Assert.Equal(2, WaitUntilHasConflict(slave, "foo/bar")["foo/bar"].Count);

            Assert.Equal(2, WaitForValue(() => GetRevisions(slave, "foo/bar").Count, 2));
            Assert.Equal(2, WaitForValue(() => GetRevisions(master, "foo/bar").Count, 2));

            SetupReplication(master, new ReplicationDocument
            {
                DocumentConflictResolution = StraightforwardConflictResolution.ResolveToLatest
            }, slave);

            Assert.True(WaitForDocument(master, "foo/bar"));
            Assert.True(WaitForDocument(slave, "foo/bar"));

            Assert.Equal(3, WaitForValue(() => GetRevisions(master, "foo/bar").Count, 3));
            Assert.Equal(3, WaitForValue(() => GetRevisions(slave, "foo/bar").Count, 3));
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
    }
}