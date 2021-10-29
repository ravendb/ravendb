using System;
using System.Linq;
using FastTests;
using Org.BouncyCastle.Asn1.Nist;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16757 : RavenTestBase
    {
        public RavenDB_16757(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            using var store = GetDocumentStore();
            PrepareData(store);
            {
                using var session = store.OpenSession();
              //  var q0 = session.Advanced.RawQuery<dynamic>("from Users where Value = 1.0").ToList();
                var q1 = session.Advanced.RawQuery<dynamic>("from Users where \"My.Name\" = \"Maciej\"").ToList();
                Assert.Equal(1, q1.Count);
                var q2 = session.Advanced.RawQuery<dynamic>("from Users where My.Name = \"Jan\"").ToList();
                Assert.Equal(1, q2.Count);

            }
            WaitForUserToContinueTheTest(store);
        }

        private void PrepareData(DocumentStore store)
        {
            var requestExecutor = store.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                var djv = new DynamicJsonValue
                {
                    ["Value"] = "1.0",
                    ["My.Name"] = "Maciej",
                    ["My"] = new DynamicJsonValue { ["Name"] = "Jan", },
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue { [Constants.Documents.Metadata.Collection] = "Users", }
                };

                var json = ctx.ReadObject(djv, "users/1");

                var putCommand = new PutDocumentCommand("users/1", null, json);

                requestExecutor.Execute(putCommand, ctx);
            }
        }
    }
}
