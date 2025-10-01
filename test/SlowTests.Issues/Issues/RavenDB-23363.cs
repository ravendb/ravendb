using System.Linq;
using FastTests;
using Lucene.Net.Analysis.Standard;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23363(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void LuceneStandardAnalyzerIsTreatedAsKnownInsteadOfCustomInSearchMethodWithWildcards(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new Document(){Name = "qwerty"});
        session.Store(new Document(){Name = "qwertz"});
        session.SaveChanges();
        new Index().Execute(store);
        Indexes.WaitForIndexing(store);

        var results = session.Query<Document, Index>().Search(x => x.Name, "qwert*").ToList();
        Assert.Equal(2, results.Count);

        results = session.Query<Document, Index>().Search(x => x.Name, "*z").ToList();
        Assert.Equal(1, results.Count);

        results = session.Query<Document, Index>().Search(x => x.Name, "*wer*").ToList();
        Assert.Equal(2, results.Count);
    }

    private class Index : AbstractIndexCreationTask<Document>
    {
        public Index()
        {
            Map = documents => documents.Select(d => new { d.Name });
            Analyze(x => x.Name, nameof(StandardAnalyzer));
        }
    }

    private class Document
    {
        public string Name { get; set; }
    }
}
