using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Tests.Core.Utils.Entities;

using Xunit;

namespace FastTests.Client.Indexing
{
    public class StaticIndexesFromClient : RavenTestBase
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
                    .AsyncDatabaseCommands
                    .PutIndexAsync("Users_ByName", input);

                var output = await store
                    .AsyncDatabaseCommands
                    .GetIndexAsync("Users_ByName");

                Assert.Equal(1, output.IndexId);
                Assert.True(input.Equals(output, compareIndexIds: false, ignoreFormatting: false, ignoreMaxIndexOutput: false));
            }
        }
    }
}