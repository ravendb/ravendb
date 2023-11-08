using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_21632 : RavenTestBase
{
    public RavenDB_21632(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CoraxFacetWillNotCountNullValuesInRangeFacets(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new TestDocumentIndex());

        using (var session = store.OpenSession())
        {
            session.Store(new TestDocument {Age = 12});
            session.Store(new TestDocument {Age = 24});
            session.Store(new TestDocument {Age = 72});
            session.Store(new TestDocument {Age = null});
            session.Store(new TestDocument {Age = null});
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (IDocumentSession session = store.OpenSession())
        {
            var results = session
                .Query<TestDocument, TestDocumentIndex>()
                .AggregateBy(facet => facet.ByRanges(d => d.Age < 1))
                .Execute();

            var values = results.Values.First().Values.First(x => x.Range == "Age < 1");

            int count = session
                .Query<TestDocument, TestDocumentIndex>()
                .Count(d => d.Age < 1);

            Assert.Equal(count, values.Count);
        }
    }


    private class TestDocument
    {
        public int? Age { get; set; }
    }

    private class TestDocumentIndex : AbstractIndexCreationTask<TestDocument>
    {
        public override string IndexName => "indexes/test_document";

        public TestDocumentIndex()
        {
            Map = docs => from doc in docs select new {doc.Age};
        }
    }
}
