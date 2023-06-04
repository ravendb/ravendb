using FastTests;
using Raven.Client.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenIntegration : RavenTestBase
{
    public RavenIntegration(ITestOutputHelper output) : base(output)
    {
    }
    
      
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AutoIndexCanOrderByFieldThatExistsOnlyInSpecificDocument_SortByIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments();

        using var session = store.OpenSession();

        //In case of AutoIndexes this should work, in static index we can reproduce the issue with `CreateField` API
        var orderByClauseQuery = session.Advanced.RawQuery<object>("from TestCollection where Name = 'Maciej' order by OrderByClause").ToList();
        Assert.Equal(5005, orderByClauseQuery.Count);
        
        orderByClauseQuery = session.Advanced.RawQuery<object>("from TestCollection where Name = 'Maciej' order by OrderByClause limit 10").ToList();
        Assert.Equal(10, orderByClauseQuery.Count); 

        void InsertDocuments()
        {
            using var bulk = store.BulkInsert();
            for (int i = 0; i < 5_000; i++)
            {
                if (i % 1000 == 0)
                {
                    bulk.Store(new {Name = "Maciej",OrderByClause = 1}, "test-v/", new MetadataAsDictionary { ["@collection"] = "TestCollection"});    
                }
                bulk.Store(new { Name = "Maciej" },"test-x/", new MetadataAsDictionary { ["@collection"] = "TestCollection"});
            }
        }
    }

}
