using System;
using System.Collections.Generic;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12333 : RavenTestBase
    {
        [Fact]
        public void CanDisableStrictMode()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Patching.StrictMode)] = "false"
            }))
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
