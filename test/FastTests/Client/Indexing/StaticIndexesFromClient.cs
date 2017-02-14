using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
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
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                };

                await store
                    .Admin
                    .SendAsync(new PutIndexesOperation(new []{input}));

                var output = await store
                    .Admin
                    .SendAsync(new GetIndexOperation("Users_ByName"));

                Assert.Equal(1, output.IndexId);
                Assert.True(input.Equals(output, compareIndexIds: false, ignoreFormatting: false));
            }
        }
    }
}