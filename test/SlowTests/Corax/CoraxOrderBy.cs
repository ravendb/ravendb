using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class CoraxOrderBy : RavenTestBase
{
    public CoraxOrderBy(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void MaybeBreakTiesCompensiveTests(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        Output.WriteLine(DateTime.Now.ToString("f"));
        session.Store(new SortingStruct("C", "2024"));
        session.Store(new SortingStruct("C", "2023"));
        session.Store(new SortingStruct("C", "2022"));
        session.SaveChanges();

        var query = session.Query<SortingStruct>()
            .Customize(x => x.WaitForNonStaleResults())
            .Where(x => x.Name == "C" && x.Year != null)
            .OrderByDescending(x => x.Year);
            
        var results = query.ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal("2024", results[0].Year);
        Assert.Equal("2023", results[1].Year);
        Assert.Equal("2022", results[2].Year);

        results = session.Query<SortingStruct>()
            .Customize(x => x.WaitForNonStaleResults())
            .Where(x => x.Name == "C" && x.Year != null)
            .OrderBy(x => x.Name)
            .ThenByDescending(x => x.Year)
            .ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal("2024", results[0].Year);
        Assert.Equal("2023", results[1].Year);
        Assert.Equal("2022", results[2].Year);
        
        results = session.Query<SortingStruct>()
            .Customize(x => x.WaitForNonStaleResults())
            .Where(x => x.Name == "C" && x.Year != null)
            .OrderByDescending(x => x.Year)
            .ThenByDescending(x => x.Name)
            .ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal("2024", results[0].Year);
        Assert.Equal("2023", results[1].Year);
        Assert.Equal("2022", results[2].Year);
    }
    
    private record SortingStruct(string Name, string Year, string Id = null);

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void CanSortDynamicFieldViaCorax(Options options)
    {
        var random = new Random(12431234);
        using var store = GetDocumentStore(options);
        var list = Enumerable.Range(0, 100).Select(i => new NumberDto() {Value = random.NextInt64()}).ToList();
        using (var bulk = store.BulkInsert())
        { 
            foreach (var l in list)
                bulk.Store(l);
        }

        var index = new IndexWithDynamic();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        {
            using var session = store.OpenSession();
            var resultFromDb = session.Advanced.DocumentQuery<NumberDto>(index.IndexName).OrderBy("DynamicValue", OrderingType.Long).ToList();
            
            foreach (var numbers in list.OrderBy(i => i.Value).Zip(resultFromDb))
                Assert.Equal(numbers.First.Value, numbers.Second.Value);
        }


    }

    private class IndexWithDynamic : AbstractIndexCreationTask<NumberDto>
    {
        public IndexWithDynamic()
        {
            Map = dtos => dtos.Select(i => new {_ = CreateField("DynamicValue", i.Value)});
        }
    }

    private class NumberDto
    {
        public string Id { get; set; }
        public long Value { get; set; }
    }
}
