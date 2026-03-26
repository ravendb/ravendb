using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25817(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanUseHighlightingWithInMethod(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        var doc = new Document { Category = "cat1", Content = "test" };
        new Index().Execute(store);
        session.Store(doc);
        session.SaveChanges();
        Indexes.WaitForIndexing(store);
        var categoryIds = new[] { "cat1", "cat2" };

            
        var results =  session
            .Query<Document, Index>()
            .Where(x => x.Category.In(categoryIds))     // ✗ In() operator
            .Search(x => x.Content, "test")
            .Highlight(x => x.Content, 128, 1, out var highlights)
            .ToList();
        Assert.NotEmpty(results);

        var fragment = highlights.GetFragments(doc.Id);
        Assert.Single(fragment);
        Assert.Equal("<b style=\"background:yellow\">test</b>", fragment[0]);
    }

    private class Index : AbstractIndexCreationTask<Document>
    {
        public Index()
        {
            Map = documents => from doc in documents
                select new { doc.Category, doc.Content };

            Store(x => x.Content, FieldStorage.Yes);
            Index(x => x.Content, FieldIndexing.Search);
            TermVector(x => x.Content, FieldTermVector.WithPositionsAndOffsets);

        }
    }
    
    private class Document
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public string Content { get; set; }
    }
}
