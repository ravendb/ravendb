using Tests.Infrastructure;
using System;
using System.Collections.Generic;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12333 : RavenTestBase
    {
        public RavenDB_12333(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanDisableStrictMode(Options options)
        {
            options.ModifyDatabaseRecord += record => record.Settings[RavenConfiguration.GetKey(x => x.Patching.StrictMode)] = "false";
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine(),
                            new OrderLine()
                        }
                    });

                    session.SaveChanges();
                }

                var operation = store.Operations.Send(new PatchByQueryOperation("from Orders o update { for(i in o.Lines) { } }"));
                operation.WaitForCompletion(TimeSpan.FromSeconds(5)); // will throw with Strict Mode
            }
        }
    }
}
