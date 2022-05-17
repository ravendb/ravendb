using FastTests;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8651 : RavenTestBase
    {
        public RavenDB_8651(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_select_and_query_field_with_dot()
        {
            using (var store = GetDocumentStore())
            {
                var requestExecuter = store.GetRequestExecutor();

                using (requestExecuter.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                {
                    var djv = new DynamicJsonValue
                    {
                        ["Na.me"] = "Fitzchak",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = "Users",
                        }
                    };

                    var json = ctx.ReadObject(djv, "users/1");

                    var putCommand = new PutDocumentCommand("users/1", null, json);

                    requestExecuter.Execute(putCommand, ctx);
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<dynamic>("from Users").ToList();

                    Assert.Equal(1, results.Count);

                    var jObject = results[0] as Newtonsoft.Json.Linq.JObject;
                    Assert.NotNull(jObject.Property("Na.me"));
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<dynamic>("from Users select 'Na.me'").ToList();

                    Assert.Equal(1, results.Count);

                    var jObject = results[0] as Newtonsoft.Json.Linq.JObject;
                    Assert.NotNull(jObject.Property("Na.me"));
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<dynamic>("from Users where 'Na.me' = 'Fitzchak'").Statistics(out var stats).WaitForNonStaleResults().ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Auto/Users/By'Na.me'", stats.IndexName);

                    var jObject = results[0] as Newtonsoft.Json.Linq.JObject;
                    Assert.NotNull(jObject.Property("Na.me"));
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<dynamic>("from Users where search('Na.me', 'Fitzchak')").Statistics(out var stats).WaitForNonStaleResults().ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Auto/Users/BySearch('Na.me')", stats.IndexName);
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<dynamic>("from Users group by 'Na.me' select count()").Statistics(out var stats).WaitForNonStaleResults().ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Auto/Users/ByCountReducedBy'Na.me'", stats.IndexName);
                }
            }
        }
    }
}
