using System;
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
            // 5000 documents, but way more ids supplied for where in cause > 20 second runtime
            var documents = Enumerable.Range(1, 5_000)
                .Select(x => new Document
                {
                    Id = "documents/" + x,
                    Number = x
                })
                .ToList();

            var ids = documents.Select(x => x.Id).ToList();
            // add more ids to make query parameters big
            ids = ids.Concat(Enumerable.Range(1, 10_000).Select(x => "documents/" + x)).ToList();

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

                    Assert.Equal(documents.Count, results.Count);
                }
                sw.Stop();

#if !MEM_GUARD_STACK
                // RavenDB-10786: Testing performance requirements during validation is not necessary and counterproductive
                // as extra validations will require readjusting this value. However, this test should be available to execute
                // to ensure memory validation will happen for the rest of the test.
                Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5));
#endif
            }
        }

        private class Document
        {
            public string Id { get; set; }
            public int Number { get; set; }
        }
    }
}
