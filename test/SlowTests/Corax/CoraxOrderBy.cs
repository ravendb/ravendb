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
