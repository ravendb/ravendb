using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using SlowTests.Bugs.Indexing;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_18377 : RavenTestBase
{
    public RavenDB_18377(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void EnumerableSavedAsValue(Options options)
    {
        using var store = GetDocumentStore(options);
        using var context = JsonOperationContext.ShortTermSingleUse();

        var djv = new DynamicJsonValue {["Coll"] = new DynamicJsonValue {["$values"] = new DynamicJsonArray(new string[] {"a", "b", "c"})}};
        var requestExecutor = store.GetRequestExecutor();
        var json = context.ReadObject(djv, "tests/1");
        requestExecutor.Execute(new PutDocumentCommand("tests/1-A", null, json), context);
        {
            using var session = store.OpenSession();
            var result = session.Advanced.RawQuery<object>("from \"@empty\" where Coll = 'a'").ToList();
            Assert.NotEmpty(result);
        }
    }


    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ArrayInIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var bulkInsert = store.BulkInsert();
            bulkInsert.Store(new Item {Name = "TestFirst", Data = new[] {1, 2, 3}}
            );
        }
        new NestedArray().Execute(store);
        Indexes.WaitForIndexing(store);

        {
            //StaticIndex
            using var session = store.OpenSession();
            var result = session.Query<Item, NestedArray>().Where(p => p.Data.Any(data => data < 10)).ToList();
            Assert.Empty(result);
            result = session.Query<Item, NestedArray>().Where(p => p.Data.Any(data => data > 10)).ToList();
            Assert.Single(result);
        }
        {
            //AutoIndex

            using var session = store.OpenSession();
            var result = session.Query<Item>().Where(p => p.Data.Any(data => data > 10)).ToList();
            Assert.Empty(result);
            result = session.Query<Item>().Where(p => p.Data.Any(data => data < 10)).ToList();
            Assert.Single(result);
        }
    }

    private class Item
    {
        public string Name { get; set; }
        public int[] Data { get; set; }
    }

    private class NestedArray : AbstractIndexCreationTask<Item>
    {
        public NestedArray()
        {
            Map = docs => from doc in docs
                select new Item {Name = doc.Name, Data = doc.Data.Select(i => i * 10).ToArray()};
        }
    }
}
