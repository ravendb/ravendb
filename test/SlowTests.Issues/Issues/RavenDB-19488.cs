using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;
using FastTests;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RavenDB_19488 : RavenTestBase
    {

        public RavenDB_19488(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task ShouldReturnEmptyResultsWhenSkippedMoreDocsThanCollectionSize(Options options)
        {
            using var store = GetDocumentStore(options);
            {
                using var session = store.OpenAsyncSession();
                await session.StoreAsync(new Data { Name = "Test" });
                await session.SaveChangesAsync();
            }
            var index = new DataIndex();
            await index.ExecuteAsync(store);
            Indexes.WaitForIndexing(store);
            {
                using var session = store.OpenAsyncSession();

                var query = session.Advanced.AsyncDocumentQuery<Data, DataIndex>()

                    .Skip(2).Take(1)
                    .SelectFields<Data>("Name") //This is mandatory for reproducing, without this statement query executes and returns empty result as expected
                    .Distinct(); //This is mandatory for reproducing, without this statement query executes and returns an empty result as expected; //Skip 2 rows (only 1 row exists in collection), for values less than 2 works as expected
                Assert.Empty(await query.ToListAsync()); // Raven.Client.Exceptions.RavenException System.IndexOutOfRangeException: Index was outside the bounds of the array.

                query = session.Advanced.AsyncDocumentQuery<Data, DataIndex>()

                    .Skip(1).Take(1)
                    .SelectFields<Data>("Name")
                    .Distinct();
                Assert.Empty(await query.ToListAsync());
            }
        }

        private class Data
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class DataIndex : AbstractIndexCreationTask<Data>
        {
            public DataIndex()
            {
                Map = datas => datas.Select(i => new { i.Name });
            }
        }
    }
}
