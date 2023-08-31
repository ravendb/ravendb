using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17854 : RavenTestBase
{
    private const string Exception = "Options are not supported in range facets.";
    
    public RavenDB_17854(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Facets)]
    public void QueryThrowsExceptionWhenOptionsAreIncludedInRangeFacet()
    {
        var optionsInJson = "{\"IncludeRemainingTerms\": true, \"PageSize\": 2147483647}";
        IRawDocumentQuery<DataForAggregation> query(IDocumentSession session) => session.Advanced.RawQuery<DataForAggregation>($"from index 'Index' select facet(Age < 23, Age >= 24, '{optionsInJson}')");
        var exception = Assert.ThrowsAny<Exception>(() => RunTest(query));

        Assert.Contains(Exception, exception.Message);
    }
    
    private void RunTest(Func<IDocumentSession, IRawDocumentQuery<DataForAggregation>> query, bool assertByField = true, bool assertByRange = true)
    {
        using var store = GetStoreWithPreparedData();
        {
            using var session = store.OpenSession();
            var results = query(session).ExecuteAggregation();
            if (assertByField)
                Assert.Equal(2, results["Name"].Values.Count);
            
            if (assertByRange)
                Assert.Equal(2, results["Age"].Values.Count);
        }
    }
    
    private IDocumentStore GetStoreWithPreparedData()
    {
        var store = GetDocumentStore();
        {
            using var session = store.OpenSession();
            session.Store(new DataForAggregation() {Age = 21, Name = "Matt"});
            session.Store(new DataForAggregation() {Age = 24, Name = "Tom"});
            session.SaveChanges();
        }

        new Index().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private class DataForAggregation
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }

    private class Index : AbstractIndexCreationTask<DataForAggregation>
    {
        public Index()
        {
            Map = enumerable => enumerable.Select(i => new DataForAggregation() {Name = i.Name, Age = i.Age});
        }
    }
}
