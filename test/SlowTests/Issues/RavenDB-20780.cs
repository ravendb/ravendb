using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using SlowTests.Core.Utils.Entities.Faceted;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20780 : RavenTestBase
{
    public RavenDB_20780(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
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
}
