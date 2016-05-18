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
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }).ConfigureAwait(false);
                    await session.StoreAsync(new User { Name = "Arek" }).ConfigureAwait(false);

                    await session.SaveChangesAsync().ConfigureAwait(false);
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
            }
        }
    }
}