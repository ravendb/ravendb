using System;
using FastTests;
using FastTests.Server.JavaScript;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15151 : RavenTestBase
    {
        public RavenDB_15151(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void Can_Use_Refresh_In_Patch(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                const string orderId = "users/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order(), "users/1");
                    session.SaveChanges();
                }

                var expires = DateTime.UtcNow.AddDays(1);
                var expiresString = expires.GetDefaultRavenFormat(isUtc: true);

                using (var session = store.OpenSession())
                {
                    session.Advanced.Defer(
                        new PatchCommandData(
                            orderId,
                            null,
                            new PatchRequest
                            {
                                Script = $"this[\"@metadata\"][\"{Constants.Documents.Metadata.Refresh}\"]=\"{expiresString}\";"
                            },
                            null));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(orderId);
                    var metadata = session.Advanced.GetMetadataFor(order);
                    var refreshValue = metadata.GetString(Constants.Documents.Metadata.Refresh);
                    Assert.Equal(expiresString, refreshValue);
                }
            }
        }
    }
}
