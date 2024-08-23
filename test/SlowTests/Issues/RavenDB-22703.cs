using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22703 : RavenTestBase
{
    public RavenDB_22703(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestQueryWithOrderByClauseAndNoWhereClause(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var bar1 = new Bar() { Foo = new Foo() { BarBool = false, BarShort = 9 } };
                var bar2 = new Bar() { Foo = new Foo() { BarBool = null } };
                var bar3 = new Bar() { Foo = null };
                
                session.Store(bar1);
                session.Store(bar2);
                session.Store(bar3);
                
                session.SaveChanges();
                
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
                {
                    var reader = context.ReadObject(new DynamicJsonValue
                    {
                        ["@metadata"] = new DynamicJsonValue{
                            ["@collection"] = "Bars",
                            ["Raven-Clr-Type"] = "SlowTests.Issues.RavenDB_22703+Bar, SlowTests"
                        }
                    }, "bars/4-A");
                    requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "bars/4-A", null, reader), context);
                }
                
                var res = session.Query<Bar>()
                    .OrderByDescending(b => b.Foo.BarBool)
                    .ThenByDescending(b => b.Foo.BarShort)
                    .ToList();

                Assert.Equal(4, res.Count);
                
                var deleteByQueryOp = new DeleteByQueryOperation("from 'Bars'");
                
                store.Operations.Send(deleteByQueryOp);
                
                Indexes.WaitForIndexing(store);
                
                res = session.Query<Bar>()
                    .OrderByDescending(b => b.Foo.BarBool)
                    .ThenByDescending(b => b.Foo.BarShort)
                    .ToList();
                
                Assert.Equal(0, res.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void TestOrderingOnShardedDatabase(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var bar1 = new Bar() { Foo = new Foo() { BarBool = false, BarShort = 9 } };
                var bar2 = new Bar() { Foo = new Foo() { BarBool = null } };
                var bar3 = new Bar() { Foo = null };
                var bar4 = new Bar() { Foo = new Foo() { BarBool = true, BarShort = 21 } };
                var bar5 = new Bar() { Foo = null };
                
                session.Store(bar1);
                session.Store(bar2);
                session.Store(bar3);
                session.Store(bar4);
                session.Store(bar5);
                
                session.SaveChanges();
                
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
                {
                    var reader = context.ReadObject(new DynamicJsonValue
                    {
                        ["@metadata"] = new DynamicJsonValue{
                            ["@collection"] = "Bars",
                            ["Raven-Clr-Type"] = "SlowTests.Issues.RavenDB_22703+Bar, SlowTests"
                        }
                    }, "bars/6");
                    requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "bars/6", null, reader), context);
                }
                
                var res = session.Query<Bar>()
                    .OrderByDescending(b => b.Foo.BarBool)
                    .ThenByDescending(b => b.Foo.BarShort)
                    .ToList();
                
                Assert.Equal(6, res.Count);
                
                Assert.Equal(true, res[0].Foo.BarBool);
                Assert.Equal(false, res[1].Foo.BarBool);
            }
        }
    }

    private class Bar
    {
        public Foo Foo { get; set; } = null!;
    }

    private class Foo
    {
        public short BarShort { get; set; }
        public bool? BarBool { get; set; }
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void TestOrderingOfDynamicFields(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var p1 = new Product() { Attributes = new Dictionary<string, object>() { { "Color", "Red" }, {"Size", 42 } } };
                var p2 = new Product() { Attributes = new Dictionary<string, object>() { { "Color", "Blue" } } };
                var p3 = new Product() { Attributes = new Dictionary<string, object>() { { "Size", 37 } } };
                
                session.Store(p1);
                session.Store(p2);
                session.Store(p3);
                
                session.SaveChanges();
                
                var index = new Products_ByAttributeKey();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var res = session
                    .Advanced
                    .DocumentQuery<Product, Products_ByAttributeKey>()
                    .OrderBy("Size")
                    .ToList();

                Assert.Equal(3, res.Count);
            }
        }
    }

    private class Product
    {
        public Dictionary<string, object> Attributes { get; set; }
    }
    
    private class Products_ByAttributeKey : AbstractIndexCreationTask<Product>
    {
        public Products_ByAttributeKey()
        {
            Map = products => from p in products
                select new
                {
                    _ = p.Attributes.Select(item => CreateField(item.Key, item.Value))
                };
        }
    }
}
