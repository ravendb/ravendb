using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Data.Indexes;
using Raven.NewClient.Operations.Databases.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client.Indexing
{
    public class StaticIndexesFromClient : RavenNewTestBase
    {
        [Fact]
        public async Task CanPut()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }

                var input = new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                };

                await store
                    .Admin
                    .SendAsync(new PutIndexOperation("Users_ByName", input));

                var output = await store
                    .Admin
                    .SendAsync(new GetIndexOperation("Users_ByName"));

                Assert.Equal(1, output.IndexId);
                Assert.True(input.Equals(output, compareIndexIds: false, ignoreFormatting: false));
            }
        }
    }
}