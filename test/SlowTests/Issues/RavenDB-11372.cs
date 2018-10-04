using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11372 : RavenTestBase
    {
        [Fact]
        public void CanGetGoodErrorForBadTopologies()
        {
            UseNewLocalServer(new Dictionary<string, string>
            {
                ["PublicServerUrl"] = "http://invalid.example.com" // intentionally broken!
            });

            var url = Server.WebUrl;
            var reqExec = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, "Foo", null, DocumentConventions.Default);
            var op = new CreateDatabaseOperation(new Raven.Client.ServerWide.DatabaseRecord("Foo"));
            using (reqExec.ContextPool.AllocateOperationContext(out var ctx))
                reqExec.Execute(op.GetCommand(DocumentConventions.Default, ctx), ctx);


            using (var store = new DocumentStore
            {
                Urls = new[] {url},
                Database = "Foo",
            })
            {
                store.Initialize();
            
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<RavenException>(() => session.Load<object>("users/1-a"));
                    Assert.Contains("invalid.example.com", e.Message);
                }
            }
        }
    }
}
