using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

// vLLM 0.31+ lets the model call tools together with response_format only for strict tools, and it reads strict inside "function"
public class RavenDB_27249 : RavenTestBase
{
    public RavenDB_27249(ITestOutputHelper output) : base(output)
    {
    }

    private class Customer
    {
        public string Name { get; set; }
        public string City { get; set; }
    }

    private class CityAnswer
    {
        public string Customer { get; set; }
        public string City { get; set; }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task StrictTools_CallToolsTogetherWithTheSchema(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Customer { Name = "Golan Nahum", City = "Hadera" }, "customers/1");
            await session.SaveChangesAsync();
        }

        var agent = new AiAgentConfiguration("orders-assistant", config.ConnectionStringName,
            "You are an assistant for an orders database. Use the tools to look up data. Never guess data.")
        {
            Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "GetCustomer",
                    Description = "Returns the customer document by its id, e.g. customers/1",
                    Query = "from Customers where id() = $id",
                    ParametersSampleObject = "{\"id\": \"customers/1\"}"
                }
            ]
        };
        var agentId = (await store.AI.CreateAgentAsync(agent, new CityAnswer { Customer = "The customer name", City = "The customer city" })).Identifier;

        var chat = store.AI.Conversation(agentId, "chats/", creationOptions: null, debug: true);
        chat.SetUserPrompt("Which city does customer customers/1 live in?");
        var result = await chat.RunAsync<CityAnswer>(CancellationToken.None);

        Assert.Equal(AiConversationResult.Done, result.Status);
        Assert.Equal("Hadera", result.Answer.City);

        using (var session = store.OpenAsyncSession())
        {
            var traces = await session.Advanced.LoadStartingWithAsync<JObject>($"{chat.Id}/{AiDebugTrace.TraceSegment}/", pageSize: 1024);
            var first = JObject.Parse(traces.First().Value<string>("RequestBody"));

            Assert.NotNull(first["response_format"]);
            Assert.All(first["tools"]!, tool =>
            {
                Assert.True(tool["function"]!.Value<bool>("strict"));
                Assert.Null(tool["strict"]);
            });
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task HandWrittenSchema_IsStrictOnlyWhenEveryObjectIsClosedAndFullyRequired()
    {
        using var store = GetDocumentStore();
        var database = await GetDatabase(store.Database);
        var handler = new ConversationHandler(Server.ServerStore, database) { Authentication = null };
        var configuration = new AiAgentConfiguration
        {
            Actions =
            [
                new AiAgentToolAction
                {
                    Name = "Open",
                    Description = "the user opted out of strict on a nested object",
                    ParametersSchema = "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"filter\":{\"type\":\"object\",\"additionalProperties\":true}},\"required\":[\"filter\"]}"
                },
                new AiAgentToolAction
                {
                    Name = "NoAdditionalProperties",
                    Description = "a missing additionalProperties means open, strict would get a 400",
                    ParametersSchema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"]}"
                },
                new AiAgentToolAction
                {
                    Name = "Optional",
                    Description = "closed, but id isn't in required, strict would get a 400",
                    ParametersSchema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[],\"additionalProperties\":false}"
                },
                new AiAgentToolAction
                {
                    Name = "Closed",
                    Description = "closed with every property required, so it goes strict",
                    ParametersSchema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}"
                }
            ]
        };

        using var pool = new JsonContextPool();
        using var client = new CapturingChatCompletionClient(pool,
            new OpenAiChatCompletionProvider(new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o")), _ => CapturingChatCompletionClient.Ok("{}"));

        using (pool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            var tools = client.PrepareTools(ctx, handler.BuildToolDescriptors(ctx, configuration));
            Assert.Equal(4, tools.Count);

            tools[0].TryGet("function", out BlittableJsonReaderObject open);
            Assert.False(open.TryGetMember("strict", out _));

            tools[1].TryGet("function", out BlittableJsonReaderObject noAdditionalProperties);
            Assert.False(noAdditionalProperties.TryGetMember("strict", out _));

            tools[2].TryGet("function", out BlittableJsonReaderObject optional);
            Assert.False(optional.TryGetMember("strict", out _));

            tools[3].TryGet("function", out BlittableJsonReaderObject closed);
            Assert.True(closed.TryGet("strict", out bool strict) && strict);
        }
    }
}
