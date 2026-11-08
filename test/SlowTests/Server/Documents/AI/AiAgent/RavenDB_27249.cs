using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
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
    public async Task HandWrittenSchema_IsStrictOnlyWhenTheUserAsks_AndStrictSurvivesSave()
    {
        const string schema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}";

        using var store = GetDocumentStore();
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = "fake-connection",
            Identifier = "fake-connection",
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings("fake-key", "http://localhost:1/", "fake-model")
        }));

        var agent = new AiAgentConfiguration("strict-tools", "fake-connection", "You answer questions about orders.")
        {
            Queries =
            [
                new AiAgentToolQuery { Name = "FromSample", Description = "d", Query = "from Orders where id() = $id", ParametersSampleObject = "{\"id\": \"orders/1\"}" },
                new AiAgentToolQuery { Name = "QueryStrict", Description = "d", Query = "from Orders where id() = $id", ParametersSchema = schema, Strict = true }
            ],
            Actions =
            [
                new AiAgentToolAction { Name = "ActionNotStrict", Description = "d", ParametersSchema = schema },
                new AiAgentToolAction { Name = "ActionStrict", Description = "d", ParametersSchema = schema, Strict = true }
            ]
        };
        await store.AI.CreateAgentAsync(agent, new CityAnswer { Customer = "name", City = "city" });

        var saved = (await store.AI.GetAgentsAsync()).AiAgents.Single();
        Assert.Equal([false, true], saved.Queries.Select(q => q.Strict));
        Assert.Equal([false, true], saved.Actions.Select(a => a.Strict));

        var database = await GetDatabase(store.Database);
        var handler = new ConversationHandler(Server.ServerStore, database) { Authentication = null };

        using var pool = new JsonContextPool();
        using var client = new CapturingChatCompletionClient(pool,
            new OpenAiChatCompletionProvider(new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o")), _ => CapturingChatCompletionClient.Ok("{}"));

        using (pool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            var tools = client.PrepareTools(ctx, handler.BuildToolDescriptors(ctx, saved));

            var strictByName = tools.ToDictionary(
                tool => tool.TryGet("function", out BlittableJsonReaderObject function) && function.TryGet("name", out string name) ? name : null,
                tool => tool.TryGet("function", out BlittableJsonReaderObject function) && function.TryGet("strict", out bool strict) && strict);

            // a schema built from a sample is always strict, a hand-written one only when the user set Strict
            Assert.True(strictByName["FromSample"]);
            Assert.True(strictByName["QueryStrict"]);
            Assert.False(strictByName["ActionNotStrict"]);
            Assert.True(strictByName["ActionStrict"]);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void GenAi_ChangingStrictOnAQueryIsAConfigurationChange()
    {
        var connectionStrings = new Dictionary<string, AiConnectionString>();

        Assert.False(GenAiConfigWithStrict(strict: true).Compare(GenAiConfigWithStrict(strict: true), connectionStrings).HasFlag(EtlConfigurationCompareDifferences.Other));
        Assert.True(GenAiConfigWithStrict(strict: true).Compare(GenAiConfigWithStrict(strict: false), connectionStrings).HasFlag(EtlConfigurationCompareDifferences.Other));
    }

    private static GenAiConfiguration GenAiConfigWithStrict(bool strict) => new()
    {
        Name = "genai-strict",
        ConnectionStringName = "genai-cs",
        Collection = "Docs",
        Prompt = "Use the query tool to answer.",
        SampleObject = "{\"Answer\":\"a\"}",
        GenAiTransformation = new GenAiTransformation { Script = "ai.genContext(this);" },
        Queries = [new AiAgentToolQuery("Q", "run the query", "from Products where id() = $id")
        {
            ParametersSchema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}",
            Strict = strict
        }]
    };
}
