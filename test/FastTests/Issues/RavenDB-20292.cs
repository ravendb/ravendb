using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_20292 : RavenTestBase
    {
        public RavenDB_20292(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task WhereIdInLargeCollectionPerformance()
        {
            // before fixing id injection to script, this could take more than two minutes
            var documents = Enumerable.Range(1, 20_000)
                .Select(x => new Document
                {
                    Id = "documents/" + x,
                    Number = x
                })
                .ToList();

            var ids = documents.Select(x => x.Id).ToList();

            using (var store = GetDocumentStore())
            {
                await using (var insert = store.BulkInsert())
                {
                    foreach (var document in documents)
                    {
                        await insert.StoreAsync(document);
                    }
                }

                var sw = Stopwatch.StartNew();
                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Query<Document>()
                        .Where(x => x.Id.In(ids))
                        .Select(x => new
                        {
                            Number = x.Number + 1
                        })
                        .ToListAsync();

                    Assert.Equal(ids.Count, results.Count);
                }
                sw.Stop();

                Output.WriteLine("Took: " + sw.Elapsed);
            }
        }

        private class Document
        {
            public string Id { get; set; }
            public int Number { get; set; }
        }
    }
}
