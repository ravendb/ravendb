using System.Linq;
using FastTests;
using Raven.Client.Documents.Commands;
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
        
    private class Bar
    {
        public Foo Foo { get; set; } = null!;
    }

    private class Foo
    {
        public short BarShort { get; set; }
        public bool? BarBool { get; set; }
    }
}
