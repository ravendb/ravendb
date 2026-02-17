using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_25673(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanEvaluateConditionalExpressionAutoIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store);
        
        using var session = store.OpenSession();
        QueryStatistics queryStatistics;
        var conditionalNameWithoutParameter = session.Advanced
            .RawQuery<Dto>("from Dtos where exists($p0, startsWith(Name, $p0), false)")
            .WaitForNonStaleResults()
            .Statistics(out queryStatistics)
            .ToList();

        Assert.Empty(conditionalNameWithoutParameter);
        Assert.Equal("Auto/Dtos/ByName", queryStatistics.IndexName);
        
        var conditionalNameWithoutParameterIncludeAll = session.Advanced
            .RawQuery<Dto>("from Dtos where exists($p0, startsWith(Name, $p0), true)")
            .WaitForNonStaleResults()
            .Statistics(out queryStatistics)
            .ToList();

        Assert.Equal(3, conditionalNameWithoutParameterIncludeAll.Count);
        Assert.Equal("Auto/Dtos/ByName", queryStatistics.IndexName);
        
        var conditionalNameWithParameter = session.Advanced
            .RawQuery<Dto>("from Dtos where exists($p0, startsWith(Name, $p0), false)")
            .AddParameter("p0", "te")
            .WaitForNonStaleResults()
            .Statistics(out queryStatistics)
            .ToList();
        Assert.Equal(2, conditionalNameWithParameter.Count);
        Assert.Contains("test", conditionalNameWithParameter[0].Name);
        Assert.Contains("test", conditionalNameWithParameter[1].Name);
        Assert.Equal("Auto/Dtos/ByName", queryStatistics.IndexName);

        
        var conditionalWithoutParameterOrStatement = session.Advanced
            .RawQuery<Dto>("from Dtos where exists($p0, startsWith(Name, $p0), false) or Value = 2")
            .WaitForNonStaleResults()
            .Statistics(out queryStatistics)
            .ToList();
        
        Assert.Single(conditionalWithoutParameterOrStatement);
        Assert.Contains("Maciej", conditionalWithoutParameterOrStatement[0].Name);
        Assert.Equal("Auto/Dtos/ByNameAndValue", queryStatistics.IndexName);

        
        var conditionalWithParameterOrStatement = session.Advanced
            .RawQuery<Dto>("from Dtos where exists($p0, startsWith(Name, $p0), false) or Value = 2")
            .AddParameter("p0", "te")
            .WaitForNonStaleResults()
            .Statistics(out queryStatistics)
            .ToList();
        
        Assert.Equal(3, conditionalWithParameterOrStatement.Count);
        Assert.Equal("Auto/Dtos/ByNameAndValue", queryStatistics.IndexName);
        
        var conditionalWithConditionInsideWithoutParameter = session.Advanced
            .RawQuery<Dto>("from Dtos where exists($p0, startsWith(Name, $p0) and Value = 1, false)")
            .WaitForNonStaleResults()
            .Statistics(out queryStatistics)
            .ToList();
        
        Assert.Empty(conditionalWithConditionInsideWithoutParameter);
        
        var conditionalWithConditionInsideWithParameter = session.Advanced
            .RawQuery<Dto>("from Dtos where exists($p0, startsWith(Name, $p0) and Value = 1, false)")
            .AddParameter($"p0", "test")
            .WaitForNonStaleResults()
            .Statistics(out queryStatistics)
            .ToList();

        Assert.Single(conditionalWithConditionInsideWithParameter);
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanEvaluateConditionalExpressionStaticIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store);
        new Index().Execute(store);
        Indexes.WaitForIndexing(store);
        
        using var session = store.OpenSession();
        var conditionalNameWithoutParameter = session.Advanced
            .RawQuery<Dto>("from index 'Index' where exists($p0, startsWith(Name, $p0), false)")
            .ToList();

        Assert.Empty(conditionalNameWithoutParameter);
        
        var conditionalNameWithoutParameterIncludeAll = session.Advanced
            .RawQuery<Dto>("from index 'Index' where exists($p0, startsWith(Name, $p0), true)")
            .ToList();

        Assert.Equal(3, conditionalNameWithoutParameterIncludeAll.Count);
        
        var conditionalNameWithParameter = session.Advanced
            .RawQuery<Dto>("from index 'Index' where exists($p0, startsWith(Name, $p0), false)")
            .AddParameter("p0", "te")
            .ToList();
        Assert.Equal(2, conditionalNameWithParameter.Count);
        Assert.Contains("test", conditionalNameWithParameter[0].Name);
        Assert.Contains("test", conditionalNameWithParameter[1].Name);

        
        var conditionalWithoutParameterOrStatement = session.Advanced
            .RawQuery<Dto>("from index 'Index' where exists($p0, startsWith(Name, $p0), false) or Value = 2")
            .ToList();
        
        Assert.Single(conditionalWithoutParameterOrStatement);
        Assert.Contains("Maciej", conditionalWithoutParameterOrStatement[0].Name);

        
        var conditionalWithParameterOrStatement = session.Advanced
            .RawQuery<Dto>("from index 'Index' where exists($p0, startsWith(Name, $p0), false) or Value = 2")
            .AddParameter("p0", "te")
            .ToList();
        
        Assert.Equal(3, conditionalWithParameterOrStatement.Count);

        var conditionalWithConditionInsideWithoutParameter = session.Advanced
            .RawQuery<Dto>("from index 'Index' where exists($p0, startsWith(Name, $p0) and Value = 1, false)")
            .ToList();
        
        Assert.Empty(conditionalWithConditionInsideWithoutParameter);
        
        var conditionalWithConditionInsideWithParameter = session.Advanced
            .RawQuery<Dto>("from index 'Index' where exists($p0, startsWith(Name, $p0) and Value = 1, false)")
            .AddParameter($"p0", "test")
            .WaitForNonStaleResults()
            .ToList();

        Assert.Single(conditionalWithConditionInsideWithParameter);
        
        var nonExistingFieldQuery = session.Advanced
            .RawQuery<Dto>("from index 'Index' where exists($p0, startsWith(DoNotIndexField, $p0), false)");
        
        var exception = Assert.Throws<Raven.Client.Exceptions.RavenException>(() => nonExistingFieldQuery.ToList());
        Assert.Contains("The field 'DoNotIndexField' is not indexed in 'Index'", exception.Message);
    }

    private static void InsertDocuments(IDocumentStore store)
    {
        using var session = store.OpenSession();
        session.Store(new Dto { Name = "test", Value = 1 });
        session.Store(new Dto { Name = "Maciej", Value = 2 });
        session.Store(new Dto { Name = "test2", Value = 3 });
        session.SaveChanges();
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Value { get; set; }
        public string DoNotIndexField { get; set; }
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => from dto in dtos
                select new
                {
                    dto.Name, dto.Value
                };
        }
    }
}
