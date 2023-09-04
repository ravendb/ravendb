using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs
{
    public class RavenDB_21180 : RavenTestBase
    {
        public RavenDB_21180(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanStreamFromExistsDirectlyFromIndex(Options options)
        {
            using var store = GetDocumentStore(options);

            await using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < 10_000; ++i)
                {
                    await bulkInsert.StoreAsync(new Dto("Maciej", i));
                }
            }

            await new Index().ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);
            using var session = store.OpenAsyncSession();
            Assert.Equal(10_000, await session.Advanced.AsyncDocumentQuery<Dto, Index>().WhereExists(x => x.Name).CountAsync());
        }

        private sealed record Dto(string Name, long Num, string Id = null);

        private class Index : AbstractIndexCreationTask<Dto>
        {
            public Index()
            {
                Map = dtos => dtos.Select(x => new { x.Name, x.Num });
            }
        }
    }
}
