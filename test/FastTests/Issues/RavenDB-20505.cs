using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_20505 : RavenTestBase
    {
        public RavenDB_20505(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task PatchingPerformanceWithLargeCollectionParameter()
        {
            var documents = Enumerable.Range(1, 2_000)
                .Select(x => new Document
                {
                    Id = "documents/" + x, Number = x
                })
                .ToList();

            // create a lookup to demonstrate large dictionary being passed in to patching
            Dictionary<string, int> lookup = new(documents.Count);
            foreach (var doc in documents)
            {
                lookup.Add(doc.Id, 42);
            }

            using (var store = GetDocumentStore())
            {
                await using (var insert = store.BulkInsert())
                {
                    foreach (var document in documents)
                    {
                        await insert.StoreAsync(document);
                    }
                }

                var query = new IndexQuery
                {
                    Query = "from Documents update { this.Number = $lookup[id(this)]; }",
                    QueryParameters = new Parameters
                    {
                        { "lookup", lookup }
                    }
                };

                var sw = Stopwatch.StartNew();

                var operation = await store.Operations.SendAsync(new PatchByQueryOperation(query));
                await operation.WaitForCompletionAsync();

                sw.Stop();

                Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), "Took more than allowed: " + sw.Elapsed);

                // check that patch worked
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Document>(documents[0].Id);
                    Assert.Equal(42, doc.Number);
                }
            }
        }

        private class Document
        {
            public string Id { get; set; }
            public int Number { get; set; }
        }
    }
}
