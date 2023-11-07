using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_21613 : RavenTestBase
{
    public RavenDB_21613(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Facets)]
    public void RangeFacetsAreEqualBetweenSearchEnginesInCaseOfNoMatches()
    {
        using IDocumentStore store = GetDocumentStore();
        store.ExecuteIndex(new TestDocumentIndex("indexes/test_document-lucene", SearchEngineType.Lucene));
        store.ExecuteIndex(new TestDocumentIndex("indexes/test_document-corax", SearchEngineType.Corax));

        Indexes.WaitForIndexing(store);

        using (IDocumentSession session = store.OpenSession())
        {
            var luceneResults = session
                .Query<TestDocument>("indexes/test_document-lucene")
                .AggregateBy(facet => facet.ByRanges(d => d.Age > 99))
                .Execute();
            
            var coraxResults = session
                .Query<TestDocument>("indexes/test_document-corax")
                .AggregateBy(facet => facet.ByRanges(d => d.Age > 99))
                .Execute();
             
            Assert.Equal(luceneResults.Values.First().Values.Count, coraxResults.Values.First().Values.Count);
        }
    }
    
    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Facets)]
    public void Test()
    {
        using IDocumentStore store = GetDocumentStore();
        store.ExecuteIndex(new TimeRangeTestIndex("indexes/test_document-lucene", SearchEngineType.Lucene));
        store.ExecuteIndex(new TimeRangeTestIndex("indexes/test_document-corax", SearchEngineType.Corax));

        using (IDocumentSession session = store.OpenSession())
        {
            session.Store(new TestDocument { Timestamp = DateTime.UtcNow.AddDays(-3.4) });
            session.Store(new TestDocument { Timestamp = DateTime.UtcNow.AddDays(-2.3) });
            session.Store(new TestDocument { Timestamp = DateTime.UtcNow.AddDays(-5.8) });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (IDocumentSession session = store.OpenSession())
        {
            DateTime start = DateTime.UtcNow;
			
            RangeFacet<TestDocument> second = new();
            second.Ranges.AddRange(GetRanges());
            RangeFacet secondFacet = second;
            
            Dictionary<string, FacetResult> results = session
                .Query<TestDocument>("indexes/test_document-lucene")
                .AggregateBy(secondFacet)
                .Execute();
            
            Dictionary<string, FacetResult> resultsCorax = session
                .Query<TestDocument>("indexes/test_document-corax")
                .AggregateBy(secondFacet)
                .Execute();


        }
    }
	
    private static IEnumerable<Expression<Func<TestDocument, bool>>> GetRanges()
    {
        DateTime start = DateTime.UtcNow.AddDays(1).Date;
        for (int count = 0; count < 365; count++)
        {
            DateTime to = start.AddDays(-count);
            DateTime from = to.AddDays(-1);
			
            Expression<Func<TestDocument, bool>> expression = d => d.Timestamp >= from && d.Timestamp < to;
            yield return expression;
        }
    }
    
    private class TestDocument
    {
        public int Age { get; set; }
        public DateTime Timestamp { get; set; }
    }

    private class TestDocumentIndex : AbstractIndexCreationTask<TestDocument>
    {
        public override string IndexName { get; }

        public TestDocumentIndex(string name, SearchEngineType searchEngineType)
        {
            IndexName = name;
            Map = docs => from doc in docs select new { doc.Age };

            SearchEngineType = searchEngineType;
        }
    }
    
    private class TimeRangeTestIndex : AbstractIndexCreationTask<TestDocument>
    {
        public override string IndexName { get; }

        public TimeRangeTestIndex(string indexName, SearchEngineType searchEngineType)
        {
            Map = docs => from doc in docs select new { doc.Timestamp };
            IndexName = indexName;
            SearchEngineType = searchEngineType;
        }
    }
}
