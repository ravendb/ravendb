using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27594 : RavenTestBase
{
    public RavenDB_27594(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public async Task OrderByAMultiValuedFieldReturns()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        await new Docs_ByCategoryAndTags().ExecuteAsync(store);

        // 5000 matches * 5 tags: the first 8192-id chunk of the Tags walk finds every document, the next chunks only meet them again
        await using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 5000; i++)
                await bulk.StoreAsync(new Doc { Category = "x", Tags = new[] { "t0", "t1", "t2", "t3", "t4" } });
        }

        Indexes.WaitForIndexing(store);

        var query = Task.Run(() =>
        {
            using var session = store.OpenSession();
            return session.Advanced.RawQuery<Doc>("from index 'Docs/ByCategoryAndTags' where Category = 'x' order by Tags").ToList();
        });

        Assert.True(query.Wait(TimeSpan.FromMinutes(1)), "the query did not return");
        Assert.Equal(5000, query.Result.Count);
        Assert.Equal(5000, query.Result.Select(x => x.Id).Distinct().Count());
    }

    private sealed class Doc
    {
        public string Id { get; set; }

        public string Category { get; set; }

        public string[] Tags { get; set; }
    }

    private sealed class Docs_ByCategoryAndTags : AbstractIndexCreationTask<Doc>
    {
        public Docs_ByCategoryAndTags()
        {
            Map = docs => from d in docs select new { d.Category, d.Tags };
        }
    }
}
