using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing
{
    public class RavenDB_25806 : RavenTestBase
    {
        public RavenDB_25806(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Can_Filter_Indexes_Progress_By_Name_And_Request_Exact_Progress()
        {
            using (var store = GetDocumentStore())
            {
                var (usersIndex, companiesIndex) = await CreateStaleIndexesAsync(store);

                // no names - progress of all indexes is returned
                var progress = await GetProgressAsync(store);
                Assert.Equal(2, progress.Length);

                // filter by index name
                progress = await GetProgressAsync(store, names: new[] { usersIndex.IndexName });
                var usersProgress = Assert.Single(progress);
                Assert.Equal(usersIndex.IndexName, usersProgress.Name);

                // unknown index name
                progress = await GetProgressAsync(store, names: new[] { "Unknown/Index" });
                Assert.Empty(progress);

                // exact progress
                progress = await GetProgressAsync(store, names: new[] { usersIndex.IndexName, companiesIndex.IndexName }, exact: true);
                Assert.Equal(2, progress.Length);

                usersProgress = progress.Single(x => x.Name == usersIndex.IndexName);
                var collectionStats = usersProgress.Collections["Users"];
                Assert.False(collectionStats.Estimated);
                Assert.Equal(3, collectionStats.NumberOfItemsToProcess);
                Assert.Equal(2, collectionStats.NumberOfTombstonesToProcess);

                var companiesProgress = progress.Single(x => x.Name == companiesIndex.IndexName);
                collectionStats = companiesProgress.Collections["Companies"];
                Assert.False(collectionStats.Estimated);
                Assert.Equal(4, collectionStats.NumberOfItemsToProcess);
                Assert.Equal(1, collectionStats.NumberOfTombstonesToProcess);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Can_Exclude_Indexes_From_Progress()
        {
            using (var store = GetDocumentStore())
            {
                var (usersIndex, companiesIndex) = await CreateStaleIndexesAsync(store);

                // the excluded index is skipped
                var progress = await GetProgressAsync(store, excludedNames: new[] { usersIndex.IndexName });
                var companiesProgress = Assert.Single(progress);
                Assert.Equal(companiesIndex.IndexName, companiesProgress.Name);

                // the exclusion is case insensitive, like the index names
                progress = await GetProgressAsync(store, excludedNames: new[] { usersIndex.IndexName.ToUpperInvariant() });
                companiesProgress = Assert.Single(progress);
                Assert.Equal(companiesIndex.IndexName, companiesProgress.Name);

                // the exclusion wins over the name filter
                progress = await GetProgressAsync(store, names: new[] { usersIndex.IndexName }, excludedNames: new[] { usersIndex.IndexName });
                Assert.Empty(progress);

                // an unknown excluded name changes nothing
                progress = await GetProgressAsync(store, excludedNames: new[] { "Unknown/Index" });
                Assert.Equal(2, progress.Length);

                // every stale index can be excluded
                progress = await GetProgressAsync(store, excludedNames: new[] { usersIndex.IndexName, companiesIndex.IndexName });
                Assert.Empty(progress);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Indexes_Progress_Command_Forwards_The_Query_Parameters()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var node = new ServerNode { Url = "http://localhost:8080", Database = "db" };

                var command = new GetIndexesProgressCommand(nodeTag: null, names: new[] { "Users/ByName", "Companies/ByName" }, exact: true, excludedNames: new[] { "Orders/ByTotal" });
                command.CreateRequest(context, node, out string url);
                Assert.Equal("http://localhost:8080/databases/db/indexes/progress?name=Users%2FByName&name=Companies%2FByName&excludeName=Orders%2FByTotal&exact=true", url);

                command = new GetIndexesProgressCommand(nodeTag: null, excludedNames: new[] { "Orders/ByTotal" });
                command.CreateRequest(context, node, out url);
                Assert.Equal("http://localhost:8080/databases/db/indexes/progress?excludeName=Orders%2FByTotal", url);

                command = new GetIndexesProgressCommand(nodeTag: null);
                command.CreateRequest(context, node, out url);
                Assert.Equal("http://localhost:8080/databases/db/indexes/progress", url);
            }
        }

        private async Task<(Users_ByName UsersIndex, Companies_ByName CompaniesIndex)> CreateStaleIndexesAsync(IDocumentStore store)
        {
            var usersIndex = new Users_ByName();
            var companiesIndex = new Companies_ByName();
            await usersIndex.ExecuteAsync(store);
            await companiesIndex.ExecuteAsync(store);

            Indexes.WaitForIndexing(store);

            await store.Maintenance.SendAsync(new StopIndexingOperation());

            using (var session = store.OpenAsyncSession())
            {
                for (var i = 1; i <= 5; i++)
                {
                    await session.StoreAsync(new User { Name = $"user-{i}" }, $"users/{i}");
                    await session.StoreAsync(new Company { Name = $"company-{i}" }, $"companies/{i}");
                }

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Delete("users/1");
                session.Delete("users/2");
                session.Delete("companies/1");
                await session.SaveChangesAsync();
            }

            return (usersIndex, companiesIndex);
        }

        private static async Task<IndexProgress[]> GetProgressAsync(IDocumentStore store, string[] names = null, bool exact = false, string[] excludedNames = null)
        {
            using (var commands = store.Commands())
            {
                var cmd = new GetIndexesProgressCommand(nodeTag: null, names, exact, excludedNames);
                await commands.ExecuteAsync(cmd);
                return cmd.Result;
            }
        }

        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from company in companies
                                   select new
                                   {
                                       company.Name
                                   };
            }
        }
    }
}
