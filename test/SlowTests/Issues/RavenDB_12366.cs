using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Extensions;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12366 : RavenTestBase
    {
        [Fact]
        public async Task MetadataTest()
        {
            using (var store = GetDocumentStore())
            {
                new DocsIndex().Execute(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Doc { Id = "doc-1", StrVal = "abc" });
                    await session.StoreAsync(new Doc { Id = "doc-2", StrVal = "abd" });
                    await session.StoreAsync(new Doc { Id = "doc-3", StrVal = "bcd" });
                    await session.SaveChangesAsync();

                    WaitForIndexing(store);

                    var results = await session.Advanced.AsyncDocumentQuery<Doc, DocsIndex>()
                        .IncludeExplanations(out Explanations explanations)
                        .Search(x => x.StrVal, "ab*")
                        .ToListAsync();

                    // should not throw in explanations

                    Assert.Equal(2, results.Count);

                    results = await session.Advanced.AsyncDocumentQuery<Doc, DocsIndex>()
                        .IncludeExplanations(new ExplanationOptions { GroupKey = "StrVal" }, out explanations)
                        .Search(x => x.StrVal, "ab*")
                        .ToListAsync();

                    Assert.Equal(2, results.Count);

                    using (var command = store.Commands())
                    {
                        var queryResult = await command.QueryAsync(new IndexQuery
                        {
                            Query = "from index 'DocsIndex' where search(StrVal, 'ab*')"
                        });

                        Assert.Equal(2, queryResult.Results.Length);
                        foreach (BlittableJsonReaderObject json in queryResult.Results)
                        {
                            var metadata = json.GetMetadata();
                            Assert.True(metadata.TryGet(Constants.Documents.Metadata.IndexScore, out float indexScore));
                            Assert.True(indexScore > 0);
                        }
                    }
                }
            }
        }

        private class Doc
        {
            public string Id { get; set; }
            public string StrVal { get; set; }
        }

        private class DocsIndex : AbstractMultiMapIndexCreationTask<Doc>
        {
            public DocsIndex()
            {
                AddMap<Doc>(docs =>
                    from doc in docs
                    select new
                    {
                        doc.Id,
                        doc.StrVal,
                    });

                Reduce = results =>
                    from result in results
                    group result by result.Id
                    into g
                    let doc = g.First()
                    select new
                    {
                        Id = g.Key,
                        StrVal = doc.StrVal ?? null,
                    };
            }
        }
    }
}
