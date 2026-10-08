using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Tests.Core.Utils.Entities;
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

        private static async Task<IndexProgress[]> GetProgressAsync(IDocumentStore store, string[] names = null, bool exact = false)
        {
            using (var commands = store.Commands())
            {
                var cmd = new GetIndexesProgressCommand(nodeTag: null, names, exact);
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
