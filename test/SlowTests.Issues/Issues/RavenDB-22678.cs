using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Server;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Sharding;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_22678 : RavenTestBase
{
    public RavenDB_22678(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Sharding)]
    public void TestOrchestratorScript()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var orchestrator = Sharding.GetOrchestrator(store.Database);

            var result = ExecuteScript(Server, orchestrator, "return orchestratorCtx.DatabaseName");

            Assert.Equal(store.Database, result.Value<string>());
        }
    }

    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Sharding)]
    public void TestOrchestratorShardCountScript()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var orchestrator = Sharding.GetOrchestrator(store.Database);

            var result = ExecuteScript(Server, orchestrator, "return orchestratorCtx.ShardCount");

            Assert.Equal(orchestrator.ShardCount, result.Value<int>());
        }
    }

    [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Sharding)]
    public async Task TestOrchestratorScriptOverHttp()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var client = store.GetRequestExecutor().HttpClient;
            using (var content = new StringContent("{\"Script\":\"return orchestratorCtx.DatabaseName\"}", Encoding.UTF8, "application/json"))
            using (var response = await client.PostAsync($"{store.Urls[0]}/admin/console?database={Uri.EscapeDataString(store.Database)}", content))
            {
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);

                var token = JsonConvert.DeserializeObject<JObject>(await response.Content.ReadAsStringAsync()).GetValue("Result");
                Assert.Equal(store.Database, token.Value<string>());
            }
        }
    }

    private static JToken ExecuteScript(RavenServer server, ShardedDatabaseContext databaseContext, string script)
    {
        var result = new AdminJsConsole(server, databaseContext).ApplyScript(new AdminJsScript(script));

        Assert.NotNull(result);
        var token = JsonConvert.DeserializeObject<JObject>(result).GetValue("Result");
        Assert.NotNull(token);
        return token;
    }
}
