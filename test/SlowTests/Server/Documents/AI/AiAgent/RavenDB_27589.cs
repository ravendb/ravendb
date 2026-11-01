using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

// Ollama can't call tools when the request also has response_format
public class RavenDB_27589 : RavenTestBase
{
    public RavenDB_27589(ITestOutputHelper output) : base(output)
    {
    }

    private class Customer
    {
        public string Name { get; set; }
        public string City { get; set; }
    }

    private class Order
    {
        public string Company { get; set; }
        public string Product { get; set; }
        public int Quantity { get; set; }
    }

    private class OrdersSummary
    {
        public static readonly OrdersSummary Sample = new()
        {
            Customer = "The customer name",
            City = "The customer city",
            TotalItems = 0,
            Products = [new ProductLine { Name = "The product name", Quantity = 0 }]
        };

        public string Customer { get; set; }
        public string City { get; set; }
        public int TotalItems { get; set; }
        public List<ProductLine> Products { get; set; }
    }

    private class ProductLine
    {
        public string Name { get; set; }
        public int Quantity { get; set; }
    }

    private class CityAnswer
    {
        public static readonly CityAnswer Sample = new() { Customer = "The customer name", City = "The customer city" };

        public string Customer { get; set; }
        public string City { get; set; }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Mock_Ollama_AsksForTheSchemaInAFinalRequestAfterAPlainTextAnswer()
    {
        using var store = GetDocumentStore();

        // Ollama sends "" next to tool_calls
        var toolCall = MockLlm.CreateToolCallResponse("RecentOrder").Replace("\"content\": null", "\"content\": \"\"");
        Assert.Contains("\"content\": \"\"", toolCall);

        var ollama = MockOllama();
        var requests = await RunMockConversationAsync(store, ollama,
        [
            toolCall,
            // a plain-text answer, sent without the schema
            """{"choices":[{"index":0,"message":{"role":"assistant","content":"I found the order"},"finish_reason":"stop"}],"usage":{"prompt_tokens":150,"completion_tokens":7,"total_tokens":157,"prompt_tokens_details":{"cached_tokens":30},"completion_tokens_details":{"reasoning_tokens":2}}}""",
            // no tool definitions, so a smaller prompt than the request before it
            MockLlm.CreateAnswerResponse("\"done\"", promptTokens: 140)
        ]);

        Assert.Equal(3, requests.Count);
        AssertSplit(requests);
        // the server doesn't add the schema to the agent's prompt
        Assert.Equal("You are a test agent.", SystemPrompt(requests[0]));

        // the plain-text answer, then the request-only ask for the final answer
        var sent = requests[2]["messages"]!.ToList();
        Assert.Equal("assistant", sent[^2].Value<string>("role"));
        Assert.Equal("I found the order", sent[^2].Value<string>("content"));
        Assert.Equal("user", sent[^1].Value<string>("role"));
        Assert.Equal(FinalAnswerPrompt, sent[^1].Value<string>("content"));

        // each request has its own debug trace
        Assert.Equal(3, (await GetRequestsAsync(store, MockConversationId)).Count);

        // only the structured answer is stored
        var messages = await GetStoredMessagesAsync(store, MockConversationId);
        Assert.Equal("You are a test agent.", messages[0].Value<string>("content"));
        Assert.Equal("tool", messages[^2].Value<string>("role"));
        Assert.Contains("done", messages.Last()["content"]!.ToString());
        Assert.Null(messages.Last()["output_schema"]);
        Assert.DoesNotContain(messages, m => m["content"]?.ToString() == "I found the order");
        Assert.DoesNotContain(messages, m => m["content"]?.ToString().Contains(FinalAnswerPrompt) == true);

        // the plain-text answer isn't counted: 100 + 140 prompt tokens, 110 + 150 in total
        using (var session = store.OpenAsyncSession())
        {
            var totalUsage = (await session.LoadAsync<JObject>(MockConversationId))["TotalUsage"]!;
            Assert.Equal(240, totalUsage.Value<long>(nameof(AiUsage.PromptTokens)));
            Assert.Equal(260, totalUsage.Value<long>(nameof(AiUsage.TotalTokens)));
        }

        var simple = await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions
        {
            ConversationId = MockConversationId,
            DetailLevel = AiConversationDetailLevel.Simple
        });
        // Simple shows one answer, the structured one
        var shown = Assert.Single(simple.Messages, m => m.Role == AiMessageRole.Assistant);
        Assert.Contains("done", shown.Content);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Mock_Ollama_PlainTextAnswerCutByTheTokenLimit_Throws()
    {
        using var store = GetDocumentStore();

        // a small model looping on the tool result until the context is full;
        // there's no third response, so a final request would fail with a different exception
        var e = await Assert.ThrowsAsync<TooManyTokensException>(() => RunMockConversationAsync(store, MockOllama(),
        [
            MockLlm.CreateToolCallResponse("RecentOrder"),
            """{"choices":[{"index":0,"message":{"role":"assistant","content":"12345678901234567890"},"finish_reason":"length"}],"usage":{"prompt_tokens":150,"completion_tokens":5000,"total_tokens":5150}}"""
        ]));
        Assert.Contains("finish_reason='length'", e.Message);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Mock_Ollama_SplitsAfterTheClientSendsAnActionResult()
    {
        using var store = GetDocumentStore();
        var ollama = MockOllama();

        // the action call stops the conversation until the client sends its result
        var requests = await RunMockConversationAsync(store, ollama, [MockLlm.CreateToolCallResponse("ConfirmOrder")]);
        Assert.Single(requests);
        Assert.NotNull(requests[0]["tools"]);
        Assert.Null(requests[0]["response_format"]);

        requests = await RunMockConversationAsync(store, ollama,
            [MockLlm.CreateAnswerResponse("\"plain text\""), MockLlm.CreateAnswerResponse("\"done\"")], actionResult: "confirmed");

        Assert.Equal(2, requests.Count);
        AssertSplit(requests);
        var sent = requests[1]["messages"]!.ToList();
        Assert.Equal("tool", sent[^3].Value<string>("role"));
        Assert.Equal("confirmed", sent[^3].Value<string>("content"));
        Assert.Equal("assistant", sent[^2].Value<string>("role"));
        Assert.Contains("plain text", sent[^2]["content"]!.ToString());
        Assert.Equal("user", sent[^1].Value<string>("role"));

        var messages = await GetStoredMessagesAsync(store, MockConversationId);
        Assert.Equal("confirmed", messages[^2].Value<string>("content"));
        Assert.Contains("done", messages.Last()["content"]!.ToString());
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Mock_OtherProviders_SendTheToolsTogetherWithTheSchema()
    {
        using var store = GetDocumentStore();
        var requests = await RunMockConversationAsync(store, provider: null,
            [MockLlm.CreateToolCallResponse("RecentOrder"), MockLlm.CreateAnswerResponse("\"done\"")]);

        Assert.Equal(2, requests.Count);
        Assert.All(requests, request =>
        {
            Assert.NotNull(request["tools"]);
            Assert.NotNull(request["response_format"]);
        });
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Mock_Ollama_AgentWithoutTools_SendsTheSchemaOnlyInResponseFormat()
    {
        using var store = GetDocumentStore();
        var ollama = MockOllama();
        var requests = await RunMockConversationAsync(store, ollama, [MockLlm.CreateAnswerResponse("\"done\"")], withTools: false);

        var request = Assert.Single(requests);
        Assert.Null(request["tools"]);
        Assert.NotNull(request["response_format"]);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Mock_Ollama_NoSchema_TheTextAnswerEndsTheTurn()
    {
        using var store = GetDocumentStore();
        var ollama = MockOllama();
        var requests = await RunMockConversationAsync(store, ollama, [MockLlm.CreateAnswerResponse("\"done\"")], noSchema: true);

        var request = Assert.Single(requests);
        Assert.NotNull(request["tools"]);
        Assert.Null(request["response_format"]);

        var messages = await GetStoredMessagesAsync(store, MockConversationId);
        Assert.Equal("none", messages.Last().Value<string>("output_schema"));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Mock_Ollama_RequestAfterTheToolIterationsRunOut_GoesWithTheSchema()
    {
        using var store = GetDocumentStore();
        var ollama = MockOllama();
        var requests = await RunMockConversationAsync(store, ollama,
            [MockLlm.CreateToolCallResponse("RecentOrder"), MockLlm.CreateAnswerResponse("\"done\"")], maxModelIterationsPerCall: 1);

        // the second request goes out like a final one: no tools, the schema in response_format
        Assert.Equal(2, requests.Count);
        Assert.Null(requests[1]["tools"]);
        Assert.NotNull(requests[1]["response_format"]);
    }

    [RavenFact(RavenTestCategory.Ai | RavenTestCategory.Etl)]
    public async Task Mock_GenAiTaskWithQueries_CountsEveryRequestInItsUsage()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var settings = new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o");

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = "fake-connection",
            Identifier = "fake-connection",
            ModelType = AiModelType.Chat,
            OpenAiSettings = settings
        }));
        store.Maintenance.Send(new AddGenAiOperation(new GenAiConfiguration
        {
            Name = "order-answer",
            Identifier = "order-answer",
            ConnectionStringName = "fake-connection",
            Collection = "Orders",
            Prompt = "Answer about the customer of this order.",
            SampleObject = "{\"Answer\":\"The answer\"}",
            GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ CustomerId: this.Company });" },
            Queries = [GetCustomerTool()],
            UpdateScript = "this.Answer = $output.Answer;"
        }));

        GenAiTask etlProcess = null;
        Assert.True(await WaitForValueAsync(() =>
        {
            etlProcess = database.EtlLoader.Processes.OfType<GenAiTask>().FirstOrDefault();
            return Task.FromResult(etlProcess != null);
        }, true));

        // the task gives its own client to every conversation
        var requests = 0;
        var mock = new MockLlm(database.DocumentsStorage.ContextPool, new OpenAiChatCompletionProvider(settings), onRequest: _ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(++requests == 1
                ? MockLlm.CreateToolCallResponse("GetCustomer", "{\"id\":\"customers/1\"}")
                : MockLlm.CreateAnswerResponse("\"done\"", promptTokens: 150))
        }, conventions: ChatCompletionClient.ConventionsToUse);
        var field = typeof(GenAiTask).GetField("_chatCompletionClient", BindingFlags.NonPublic | BindingFlags.Instance);
        ((ChatCompletionClient)field!.GetValue(etlProcess))?.Dispose();
        field.SetValue(etlProcess, mock);

        var etlDone = Etl.WaitForEtlToComplete(store);
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Order { Company = "customers/1", Product = "Chai", Quantity = 1 }, "orders/1");
            await session.SaveChangesAsync();
        }
        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));
        Assert.Equal(2, requests);

        // both requests, not only the last one
        var load = etlProcess.GetPerformanceStats().First(x => x.NumberOfLoadedItems > 0).Details.Operations[^1];
        var usage = ((GenAiPerformanceOperation)load.Operations.First(x => x.Name == GenAiOperations.LoadToModel)).Usage;
        Assert.Equal(250, usage.PromptTokens);
        Assert.Equal(20, usage.CompletionTokens);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task TwoTools_ReturnsTheDataFromTheDatabase_AndSplitsTheRequests(Options options, GenAiConfiguration config)
    {
        using var store = await CreateStoreWithOrdersAsync(options, config);
        var agentId = await CreateAgentWithToolsAsync(store, config, OrdersSummary.Sample);

        var chat = store.AI.Conversation(agentId, "chats/", creationOptions: null, debug: true);
        chat.SetUserPrompt("Summarize the orders of customer customers/1.");
        var result = await chat.RunAsync<OrdersSummary>(CancellationToken.None);

        Assert.Equal(AiConversationResult.Done, result.Status);
        Assert.Equal("Golan Nahum", result.Answer.Customer);
        Assert.Equal("Hadera", result.Answer.City);
        Assert.Equal(6, result.Answer.TotalItems);
        Assert.Equal(3, result.Answer.Products.Count);

        var requests = await GetRequestsAsync(store, chat.Id);
        AssertSplit(requests);

        // the final request sees the plain-text answer, which isn't stored
        var sent = requests.Last()["messages"]!.ToList();
        Assert.Equal("assistant", sent[^2].Value<string>("role"));
        Assert.Equal(FinalAnswerPrompt, sent[^1].Value<string>("content"));

        var messages = await GetStoredMessagesAsync(store, chat.Id);
        Assert.DoesNotContain(messages, m => m.Value<string>("output_schema") == "none");
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Streaming_StreamsOnlyTheFinalStructuredAnswer(Options options, GenAiConfiguration config)
    {
        using var store = await CreateStoreWithOrdersAsync(options, config);
        var agentId = await CreateAgentWithToolsAsync(store, config, CityAnswer.Sample);

        var chat = store.AI.Conversation(agentId, "chats/", creationOptions: null, debug: true);
        chat.SetUserPrompt("Which city does customer customers/1 live in?");

        var streamed = new List<string>();
        var result = await chat.StreamAsync<CityAnswer>(nameof(CityAnswer.City), chunk =>
        {
            streamed.Add(chunk);
            return Task.CompletedTask;
        }, CancellationToken.None);

        Assert.Equal(AiConversationResult.Done, result.Status);
        Assert.Equal("Hadera", result.Answer.City);
        Assert.Equal("Hadera", string.Concat(streamed).Trim('"'));

        var requests = await GetRequestsAsync(store, chat.Id);
        AssertSplit(requests);

        Assert.All(requests.SkipLast(1), r => Assert.Null(r["stream"]));
        Assert.True(requests.Last().Value<bool>("stream"));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task SubAgentWithTools_ReturnsItsStructuredAnswerToTheParent(Options options, GenAiConfiguration config)
    {
        using var store = await CreateStoreWithOrdersAsync(options, config);

        // only the sub-agent has the tool
        var sub = new AiAgentConfiguration("customer-lookup", config.ConnectionStringName,
            "You look up customers in an orders database. Use the tools to look up data. Never guess data." + SchemaInPrompt(CityAnswer.Sample))
        {
            Identifier = "customer-lookup",
            Queries = [GetCustomerTool()]
        };
        var subId = (await store.AI.CreateAgentAsync(sub, CityAnswer.Sample)).Identifier;

        var parent = new AiAgentConfiguration("host", config.ConnectionStringName,
            "You answer questions about customers. You can't look data up yourself: ask the customer-lookup sub-agent and use its answer." + SchemaInPrompt(CityAnswer.Sample))
        {
            Identifier = "host",
            SubAgents = [new AiAgentToolSubAgent { Identifier = subId, Description = "Looks up a customer by id and returns the customer's name and city." }]
        };
        var parentId = (await store.AI.CreateAgentAsync(parent, CityAnswer.Sample)).Identifier;

        var chat = store.AI.Conversation(parentId, "chats/", creationOptions: null, debug: true);
        chat.SetUserPrompt("Which city does customer customers/1 live in?");
        var result = await chat.RunAsync<CityAnswer>(CancellationToken.None);

        Assert.Equal(AiConversationResult.Done, result.Status);
        Assert.Equal("Hadera", result.Answer.City);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAiTaskWithQueries_WritesTheCityFromTheDatabase(Options options, GenAiConfiguration config)
    {
        using var store = await CreateStoreWithOrdersAsync(options, config);

        config.Name = "order-city";
        config.Identifier = "order-city";
        config.Collection = "Orders";
        config.SampleObject = JsonConvert.SerializeObject(new { City = "The customer city" });
        config.Prompt = "Find the city of the customer who placed this order. Use the tools to look up data. Never guess data." + SchemaInPrompt(config.SampleObject);
        config.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ CustomerId: this.Company });" };
        config.Queries = [GetCustomerTool()];
        config.UpdateScript = "this.City = $output.City;";

        var etlDone = Etl.WaitForEtlToComplete(store);
        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        // GenAI tasks only pick up new documents by default
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Order { Company = "customers/1", Product = "Chang", Quantity = 1 }, "orders/4");
            await session.SaveChangesAsync();
        }

        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(5)));

        using (var session = store.OpenAsyncSession())
        {
            var order = await session.LoadAsync<JObject>("orders/4");
            Assert.Equal("Hadera", order.Value<string>("City"));
        }
    }

    private async Task<DocumentStore> CreateStoreWithOrdersAsync(Options options, GenAiConfiguration config)
    {
        var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new Customer { Name = "Golan Nahum", City = "Hadera" }, "customers/1");
        await session.StoreAsync(new Order { Company = "customers/1", Product = "Chai", Quantity = 3 }, "orders/1");
        await session.StoreAsync(new Order { Company = "customers/1", Product = "Tofu", Quantity = 1 }, "orders/2");
        await session.StoreAsync(new Order { Company = "customers/1", Product = "Ikura", Quantity = 2 }, "orders/3");
        await session.SaveChangesAsync();

        return store;
    }

    private static async Task<string> CreateAgentWithToolsAsync<TSchema>(IDocumentStore store, GenAiConfiguration config, TSchema sample)
    {
        var agent = new AiAgentConfiguration("orders-assistant", config.ConnectionStringName,
            "You are an assistant for an orders database. Use the tools to look up data. Never guess data. If a value can't be found, say so." + SchemaInPrompt(sample))
        {
            Queries =
            [
                GetCustomerTool(),
                new AiAgentToolQuery
                {
                    Name = "GetOrders",
                    Description = "Returns the orders of a customer by the customer id, e.g. customers/1",
                    Query = "from Orders where Company = $customerId",
                    ParametersSampleObject = "{\"customerId\": \"customers/1\"}"
                }
            ]
        };

        return (await store.AI.CreateAgentAsync(agent, sample)).Identifier;
    }

    private static AiAgentToolQuery GetCustomerTool() => new()
    {
        Name = "GetCustomer",
        Description = "Returns the customer document by its id, e.g. customers/1",
        Query = "from Customers where id() = $id",
        ParametersSampleObject = "{\"id\": \"customers/1\"}"
    };

    private static async Task<List<JObject>> GetRequestsAsync(IDocumentStore store, string conversationId)
    {
        using var session = store.OpenAsyncSession();
        var traces = await session.Advanced.LoadStartingWithAsync<JObject>($"{conversationId}/{AiDebugTrace.TraceSegment}/", pageSize: 1024);
        return traces.Select(t => JObject.Parse(t.Value<string>("RequestBody"))).ToList();
    }

    private const string MockConversationId = "chats/mock";
    private const string FinalAnswerPrompt = "Reply now with your final answer as JSON matching the schema.";

    private static OllamaChatCompletionProvider MockOllama() => new(new OllamaSettings("http://localhost:11434", "qwen"));

    // sends a user prompt, or the result of the open action call when actionResult is set
    private async Task<List<JObject>> RunMockConversationAsync(IDocumentStore store, AbstractChatCompletionProvider provider, string[] responses, string actionResult = null, bool withTools = true, int? maxModelIterationsPerCall = null, bool noSchema = false)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var agent = new AiAgentConfiguration("mock-agent", "fake-connection", "You are a test agent.")
        {
            Identifier = "mock-agent",
            MaxModelIterationsPerCall = maxModelIterationsPerCall,
            SampleObject = "{\"Answer\":\"The answer\"}",
            Queries = [new AiAgentToolQuery("RecentOrder", "Get recent orders", "from Orders limit 5") { ParametersSampleObject = "{}" }],
            Actions = [new AiAgentToolAction { Name = "ConfirmOrder", Description = "Ask the user to confirm the order", ParametersSampleObject = "{}" }]
        };

        if (withTools == false)
        {
            agent.Queries = [];
            agent.Actions = [];
        }

        var requests = new List<JObject>();
        var handler = new MockLlmConversationHandler(Server.ServerStore, database, onRequest: payload =>
        {
            requests.Add(payload);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(responses[requests.Count - 1]) };
        }, clientSettings: provider)
        {
            Authentication = null
        };

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            // a later call continues the same conversation
            string changeVector;
            using (context.OpenReadTransaction())
                changeVector = database.DocumentsStorage.Get(context, MockConversationId)?.ChangeVector;

            var body = new RequestBody { CreationOptions = new AiConversationCreationOptions(), OutputOptions = noSchema ? new AiServerOutputOptions { NoSchema = true } : null };
            if (actionResult == null)
            {
                body.UserPrompt = "Get my orders";
            }
            else
            {
                var response = new AiAgentActionResponse { ToolId = "call_mock", Content = actionResult }; // the id MockLlm gives its tool calls
                context.ReadObject(new DynamicJsonValue { ["array"] = new DynamicJsonArray { response.ToJson() } }, "mock/action-responses")
                    .TryGet("array", out BlittableJsonReaderArray actionResponses);
                body.ActionResponses = actionResponses;
            }

            handler.Initialize(agent, MockConversationId, body, changeVector, debugOverride: true);
            await handler.HandleRequestAsync(context, CancellationToken.None);
        }

        return requests;
    }

    private static async Task<List<JObject>> GetStoredMessagesAsync(IDocumentStore store, string conversationId)
    {
        using var session = store.OpenAsyncSession();
        var conversation = await session.LoadAsync<JObject>(conversationId);
        return conversation["Messages"]!.Children<JObject>().ToList();
    }

    // as Ollama recommends: the tool requests go without response_format
    private static string SchemaInPrompt(object sample)
    {
        var schema = JToken.Parse(ChatCompletionClient.GetSchemaForTool(schema: null, sampleObject: sample as string ?? JsonConvert.SerializeObject(sample))).ToString(Formatting.None);
        return "\nUse the tools and actions you need first, then reply. Your final answer must be JSON matching this schema: " + schema;
    }

    private static void AssertSplit(List<JObject> requests)
    {
        Assert.True(requests.Count >= 2, $"Expected a split into tool turns and a final turn, got {requests.Count} request(s)");

        foreach (var toolTurn in requests.SkipLast(1))
        {
            Assert.NotNull(toolTurn["tools"]);
            Assert.Null(toolTurn["response_format"]);
        }

        var finalTurn = requests.Last();
        Assert.Null(finalTurn["tools"]);
        Assert.NotNull(finalTurn["response_format"]);
    }

    private static string SystemPrompt(JObject request)
    {
        var systemMessage = request["messages"]![0]!;
        Assert.Equal("system", systemMessage.Value<string>("role"));
        return systemMessage["content"]!.ToString();
    }
}
