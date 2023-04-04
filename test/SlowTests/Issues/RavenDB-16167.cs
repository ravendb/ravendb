using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_16167: RavenTestBase
{
    public RavenDB_16167(ITestOutputHelper output) : base(output)
    {
        
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CheckIfSpreadOperatorWorksForJsIndex(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            Order o1 = new() { Price = 21 };
            Order o2 = new() { Price = 37 };
            
            session.Store(o1);
            session.Store(o2);
            
            var index = new SpreadOperatorIndex();
            
            var query = $@"from index ""{index.IndexName}"" as o select Result";
            
            index.Execute(store);

            Indexes.WaitForIndexing(store);

            session.SaveChanges();
            
            var res = session.Advanced
                .RawQuery<Dto>(query)
                .WaitForNonStaleResults().ToList();
            
            Assert.Equal(6, res.First().Result[0]);
        }
    }

    private class SpreadOperatorIndex : AbstractJavaScriptIndexCreationTask
    {
        public SpreadOperatorIndex()
        {
            Maps = new HashSet<string>(new string[]{
@"map(""Orders"", (order) => {
    var result = [];
    var a = [1, 2, 3];
    var x = function(a, b, c){
        return a + b + c;
    }
    result.push(x(...a));
    return {
        Result: result
    };
})"});

            Fields = new Dictionary<string, IndexFieldOptions>() { { "Result", new IndexFieldOptions() { Storage = FieldStorage.Yes } } };
        }
    }

    private class Order
    {
        public string Id { get; set; }
        public int Price { get; set; }
    }

    private class Dto
    {
        public string Id { get; set; }
        public double[] Result { get; set; }
    }
}
