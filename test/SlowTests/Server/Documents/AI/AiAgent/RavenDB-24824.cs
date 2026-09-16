using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_24824(ITestOutputHelper output) : RavenTestBase(output)
{
    private class DefaultSchema
    {
        public string Answer { get; set; }
    }

    private class AlternativeSchema
    {
        public string Summary { get; set; }
        public int Score { get; set; }
    }

    private class Chat
    {
        public string Id { get; set; }
        public List<Message> Messages { get; set; }
    }

    private class Message
    {
        [JsonProperty("role")]
        public string Role { get; set; }

        [JsonProperty("content")]
        public object Content { get; set; }

        [JsonProperty("output_schema")]
        public string OutputSchema { get; set; }
    }

    private static AiAgentConfiguration BuildSimpleAgent(string connectionStringName) =>
        new AiAgentConfiguration("test-assistant", connectionStringName,
            "You are a helpful assistant. Answer questions concisely.");

    [RavenFact(RavenTestCategory.Ai)]
    public async Task InvalidOutputOptionsCombinationsShouldThrow()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("agents/test", "chats/123", new AiConversationCreationOptions());
        chat.SetUserPrompt("hello");

        var ex1 = Assert.Throws<InvalidOperationException>(() =>
            chat.RunWithSchema<AlternativeSchema>(new AiOutputOptions { NoSchema = true }));
        Assert.Contains(nameof(AiOutputOptions.NoSchema), ex1.Message);

        var ex2 = Assert.Throws<InvalidOperationException>(() =>
            chat.RunWithSchema<string>(new AiOutputOptions("{}")));
        Assert.Contains("raw string", ex2.Message);

        var ex3 = Assert.Throws<InvalidOperationException>(() =>
            chat.RunWithSchema<string>(new AiOutputOptions(new AlternativeSchema { Summary = "x", Score = 1 })));
        Assert.Contains("raw string", ex3.Message);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            chat.RunWithSchemaAsync<AlternativeSchema>(new AiOutputOptions { NoSchema = true }));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task OutputSchemaFieldTrackedPerTurnInDocument(Options options, GenAiConfiguration config)
    {
        // Covers: SampleObject (.NET object) override, explicit OutputSchema override, NoSchema,
        // multi-turn tracking, no schema contamination between turns, cross-type SampleObject.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agentId = (await store.AI.CreateAgentAsync(BuildSimpleAgent(config.ConnectionStringName),
            new DefaultSchema { Answer = "the answer" })).Identifier;

        string explicitSchema = """
            {
                "name": "alt_output",
                "strict": true,
                "schema": {
                    "type": "object",
                    "properties": {
                        "Summary": { "type": "string" },
                        "Score":   { "type": "integer" }
                    },
                    "required": ["Summary", "Score"],
                    "additionalProperties": false
                }
            }
            """;

        var chat = store.AI.Conversation(agentId, "chats/1", new AiConversationCreationOptions());

        // Turn 1: default schema — no output_schema stored
        chat.SetUserPrompt("Say hello.");
        await chat.RunAsync<DefaultSchema>();

        // Turn 2: SampleObject as a .NET object, TAnswer differs from SampleObject type (cross-type)
        chat.SetUserPrompt("Rate how helpful you are on a scale of 1 to 10 and summarize what you do.");
        await chat.RunWithSchemaAsync<AlternativeSchema>(new AiOutputOptions(new AlternativeSchema { Summary = "a short summary", Score = 5 }));

        // Turn 3: explicit OutputSchema string
        chat.SetUserPrompt("Rate your usefulness 1-10 and summarize in one sentence.");
        var r3 = await chat.RunWithSchemaAsync<AlternativeSchema>(new AiOutputOptions(explicitSchema));
        Assert.NotNull(r3.Answer?.Summary);
        Assert.True(r3.Answer.Score > 0);

        // Turn 4: NoSchema — raw string output
        chat.SetUserPrompt("Say goodbye.");
        var r4 = await chat.RunAsync<string>();
        Assert.NotEmpty(r4.Answer);

        // Turn 5: back to default — no contamination from previous override turns
        chat.SetUserPrompt("What did we talk about?");
        await chat.RunAsync<DefaultSchema>();

        using var session = store.OpenAsyncSession();
        var doc = await session.LoadAsync<Chat>("chats/1");
        var msgs = doc.Messages.Where(m => m.Role == "assistant").ToList();
        Assert.Equal(5, msgs.Count);

        Assert.Null(msgs[0].OutputSchema);                          // default: no field
        Assert.Null(msgs[1].OutputSchema);                          // SampleObject override: structured, not stored
        Assert.Null(msgs[2].OutputSchema);                          // explicit schema override: structured, not stored
        Assert.Equal("none", msgs[3].OutputSchema);                 // NoSchema: output_schema stored as "none"
        Assert.Null(msgs[4].OutputSchema);                          // default again: no contamination
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task NoSchemaShorthandsReturnRawString(Options options, GenAiConfiguration config)
    {
        // Covers: Run() sync shorthand and RunAsync() no-arg shorthand both return raw string output.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agentId = (await store.AI.CreateAgentAsync(BuildSimpleAgent(config.ConnectionStringName),
            new DefaultSchema { Answer = "the answer" })).Identifier;

        var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());

        chat.SetUserPrompt("Say hello in one sentence.");
        var r1 = chat.Run();
        Assert.Equal(AiConversationResult.Done, r1.Status);
        Assert.NotEmpty(r1.Answer);

        chat.SetUserPrompt("Say hello again in one sentence.");
        var r2 = await chat.RunAsync();
        Assert.Equal(AiConversationResult.Done, r2.Status);
        Assert.NotEmpty(r2.Answer);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task SyncStreamOverloadsWork(Options options, GenAiConfiguration config)
    {
        // Covers: Stream(Action<string>) no-schema shorthand and Stream<TAnswer>(Expression, Action) structured overload.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agentId = (await store.AI.CreateAgentAsync(BuildSimpleAgent(config.ConnectionStringName),
            new DefaultSchema { Answer = "the answer" })).Identifier;

        var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());

        // Stream(Action<string>) — no-schema shorthand
        var streamed1 = new System.Text.StringBuilder();
        chat.SetUserPrompt("Say hello in one sentence.");
        var r1 = chat.Stream(chunk => streamed1.Append(chunk));
        Assert.Equal(AiConversationResult.Done, r1.Status);
        Assert.NotEmpty(r1.Answer);
        Assert.Equal(streamed1.ToString(), r1.Answer);

        // Stream<TAnswer>(Expression<Func<TAnswer,string>>, Action<string>) — structured with expression selector
        var streamed2 = new System.Text.StringBuilder();
        chat.SetUserPrompt("Say goodbye in one sentence.");
        var r2 = chat.Stream<DefaultSchema>(x => x.Answer, chunk => streamed2.Append(chunk));
        Assert.Equal(AiConversationResult.Done, r2.Status);
        Assert.NotNull(r2.Answer?.Answer);
        Assert.Equal(streamed2.ToString(), r2.Answer.Answer);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task StreamingNoSchemaPathWorks(Options options, GenAiConfiguration config)
    {
        // Covers: StreamAsync<string> without options routes to NoSchema (regression for last commit fix),
        // and StreamAsync(callback) shorthand both stream and return the full text.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agentId = (await store.AI.CreateAgentAsync(BuildSimpleAgent(config.ConnectionStringName),
            new DefaultSchema { Answer = "the answer" })).Identifier;

        var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());

        // StreamAsync<string> without AiOutputOptions must route to NoSchema, not try to parse structured JSON
        var streamed1 = new StringBuilder();
        chat.SetUserPrompt("Say hello in one sentence.");
        var r1 = await chat.StreamAsync<string>(string.Empty, chunk => { streamed1.Append(chunk); return Task.CompletedTask; });
        Assert.Equal(AiConversationResult.Done, r1.Status);
        Assert.Equal(streamed1.ToString(), r1.Answer);
        Assert.NotEmpty(r1.Answer);

        // StreamAsync(callback) shorthand
        var streamed2 = new StringBuilder();
        chat.SetUserPrompt("Tell me a short fact about databases.");
        var r2 = await chat.StreamAsync(chunk => { streamed2.Append(chunk); return Task.CompletedTask; });
        Assert.Equal(AiConversationResult.Done, r2.Status);
        Assert.Equal(streamed2.ToString(), r2.Answer);
        Assert.NotEmpty(r2.Answer);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ConvenienceOverloadsWork(Options options, GenAiConfiguration config)
    {
        // Covers: RunAsync<TAnswer>(TAnswer sampleObject) and RunAsync<TAnswer>(string schema) convenience overloads.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agentId = (await store.AI.CreateAgentAsync(BuildSimpleAgent(config.ConnectionStringName),
            new DefaultSchema { Answer = "the answer" })).Identifier;

        var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());

        // sampleObject overload
        chat.SetUserPrompt("Rate how helpful you are on a scale of 1 to 10 and summarize what you do.");
        var r1 = await chat.RunWithSchemaAsync(new AlternativeSchema { Summary = "a short summary", Score = 5 });
        Assert.Equal(AiConversationResult.Done, r1.Status);
        Assert.NotNull(r1.Answer?.Summary);
        Assert.True(r1.Answer.Score > 0);
        Assert.NotNull(r1.Usage);
        Assert.True(r1.Usage.TotalTokens > 0);
        Assert.True(r1.Elapsed > TimeSpan.Zero);

        // schema string overload
        string schemaJson = """
            {
                "name": "alt_output",
                "strict": true,
                "schema": {
                    "type": "object",
                    "properties": {
                        "Summary": { "type": "string" },
                        "Score":   { "type": "integer" }
                    },
                    "required": ["Summary", "Score"],
                    "additionalProperties": false
                }
            }
            """;

        chat.SetUserPrompt("Rate your usefulness again 1-10 and summarize in one sentence.");
        var r2 = await chat.RunWithSchemaAsync<AlternativeSchema>(schemaJson);
        Assert.Equal(AiConversationResult.Done, r2.Status);
        Assert.NotNull(r2.Answer?.Summary);
        Assert.True(r2.Answer.Score > 0);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanHandleNoArgsTool(Options options, GenAiConfiguration config)
    {
        // Covers: Handle(name, Func<object>) and Handle<TResult>(name, Func<Task<TResult>>) no-args overloads.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("time assistant", config.ConnectionStringName,
            "You are a helpful assistant. When asked for the current time, you MUST call the get_current_time tool first, then answer.");
        agent.Actions =
        [
            new AiAgentToolAction("get_current_time", "Returns the current UTC time. Takes no parameters.")
            {
                ParametersSampleObject = "{}"
            }
        ];

        var agentId = (await store.AI.CreateAgentAsync(agent, new DefaultSchema { Answer = "the answer" })).Identifier;

        var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());

        bool toolCalled = false;
        chat.Handle("get_current_time", () =>
        {
            toolCalled = true;
            return (object)DateTime.UtcNow.ToString("O");
        });

        chat.SetUserPrompt("What is the current time? Use the get_current_time tool.");
        var result = await chat.RunAsync<DefaultSchema>();

        Assert.Equal(AiConversationResult.Done, result.Status);
        Assert.True(toolCalled, "The no-args tool handler was not called");
        Assert.NotNull(result.Answer?.Answer);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task MultiTurnConversationResumesCorrectlyAfterNoSchemaTurn(Options options, GenAiConfiguration config)
    {
        // After a NoSchema turn, the next turn with the default schema must still produce structured output.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agentId = (await store.AI.CreateAgentAsync(BuildSimpleAgent(config.ConnectionStringName),
            new DefaultSchema { Answer = "the answer" })).Identifier;

        var chat = store.AI.Conversation(agentId, "chats/1", new AiConversationCreationOptions());

        chat.SetUserPrompt("Say hello.");
        var turn1 = await chat.RunAsync<string>();
        Assert.Equal(AiConversationResult.Done, turn1.Status);
        Assert.NotNull(turn1.Answer);

        chat.SetUserPrompt("Repeat the greeting you just said.");
        var turn2 = await chat.RunAsync<DefaultSchema>();
        Assert.Equal(AiConversationResult.Done, turn2.Status);
        Assert.NotNull(turn2.Answer?.Answer);
        Assert.NotEmpty(turn2.Answer.Answer);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanStreamWithSchemaOverride(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildSimpleAgent(config.ConnectionStringName);
        var r = await store.AI.CreateAgentAsync(agent, new DefaultSchema { Answer = "the answer" });

        var chat = store.AI.Conversation(r.Identifier, "chats/", new AiConversationCreationOptions());

        var streamed = new StringBuilder();
        chat.SetUserPrompt("Rate how helpful you are on a scale of 1 to 10 and summarize what you do.");
        var run = await chat.StreamWithSchemaAsync<AlternativeSchema>(
            nameof(AlternativeSchema.Summary),
            chunk =>
            {
                streamed.Append(chunk);
                return Task.CompletedTask;
            },
            new AlternativeSchema { Summary = "a short summary", Score = 5 });

        Assert.Equal(AiConversationResult.Done, run.Status);
        Assert.NotNull(run.Answer);
        Assert.NotNull(run.Answer.Summary);
        Assert.True(run.Answer.Score > 0);
        Assert.Equal(streamed.ToString(), run.Answer.Summary);
        Assert.NotEmpty(streamed.ToString());
    }
}
