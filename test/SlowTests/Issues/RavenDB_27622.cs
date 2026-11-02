using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27622 : RavenTestBase
{
    public RavenDB_27622(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AllInMatchesNullAmongMoreThanFourValues(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Doc { Tags = new[] { "a", "b", "c", "d", null } }, "docs/1");
            session.Store(new Doc { Tags = new[] { "a", "b", "c", "d" } }, "docs/2");
            session.SaveChanges();
        }

        new DocsIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<Doc>($"from index '{new DocsIndex().IndexName}' where Tags all in ('a', 'b', 'c', 'd', null)").ToList();
            Assert.Equal(new[] { "docs/1" }, results.Select(x => x.Id));
        }
    }

    private class DocsIndex : AbstractIndexCreationTask<Doc>
    {
        public DocsIndex()
        {
            Map = docs => from doc in docs
                select new { doc.Tags };
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
    }
}
