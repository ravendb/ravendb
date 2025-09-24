using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities.Faceted;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20780 : RavenTestBase
{
    public RavenDB_20780(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Patching)]
    public void PatchStatementsCountOverflowExceptionLimit()
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(new Order());
            session.SaveChanges();
        }
        var query = new IndexQuery
        {
            Query = @"
from ""Orders"" update {
    for (var i = 0; i < 10000; i++){
        this.Count = 0;
    }
}",
        };

        var exception = Assert.Throws<Raven.Client.Exceptions.Documents.Patching.JavaScriptException>(() =>
        {
            var operation =  store.Operations.Send(new PatchByQueryOperation(query));
            operation.WaitForCompletion();
        });

        Assert.Contains("The maximum number of statements executed have been reached - 10000. You can configure it by modifying the configuration option: 'Patching.MaxStepsForScript'.",
            exception.ToString());
    }
    
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
    public void JsIndexStepsCountOverflowExceptionLimit()
    {
        var options = new Options() { ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MaxStepsForScript)] = "1" };
        
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Order());
                session.SaveChanges();

                var index = new DummyIndex();
                
                index.Execute(store);
                
                var indexErrors = Indexes.WaitForIndexingErrors(store);

                Assert.Single(indexErrors);
                
                Assert.Contains("The maximum number of statements executed has been reached. You can configure it by modifying the configuration option: 'Indexing.MaxStepsForScript'.", indexErrors[0].Errors[0].Error);
            }
        }
    }

    private class DummyIndex : AbstractJavaScriptIndexCreationTask
    {
        public DummyIndex()
        {
            Maps = new HashSet<string>() 
            {
                @"map('Orders', o => {
                        for (var i = 0; i < 10; i++) {
                            var x = metadataFor(o);
                        }
                        return {
                            Blabla: x,
                            Tax: o.Tax
                        };
                    });" 
            };
        }
    }
}
