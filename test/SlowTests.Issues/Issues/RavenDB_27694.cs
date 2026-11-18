using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27694 : RavenTestBase
{
    public RavenDB_27694(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void BoostedNullEqualityMatchesNullValues(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Doc { Name = null }, "docs/null");
            session.Store(new Doc { Name = "x" }, "docs/x");
            session.SaveChanges();
        }

        new DocsIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var isNull = session.Advanced.RawQuery<Doc>($"from index '{new DocsIndex().IndexName}' where boost(Name = null, 2)").ToList();
            Assert.Equal(new[] { "docs/null" }, isNull.Select(x => x.Id));

            var notNull = session.Advanced.RawQuery<Doc>($"from index '{new DocsIndex().IndexName}' where boost(Name != null, 2)").ToList();
            Assert.Equal(new[] { "docs/x" }, notNull.Select(x => x.Id));

            var byScore = session.Advanced.RawQuery<Doc>($"from index '{new DocsIndex().IndexName}' where Name = null order by score()").ToList();
            Assert.Equal(new[] { "docs/null" }, byScore.Select(x => x.Id));
        }
    }

    private class DocsIndex : AbstractIndexCreationTask<Doc>
    {
        public DocsIndex()
        {
            Map = docs => from doc in docs
                select new { doc.Name };
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
