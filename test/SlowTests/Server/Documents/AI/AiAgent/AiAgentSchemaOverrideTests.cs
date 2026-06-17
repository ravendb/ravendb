using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class AiAgentSchemaOverrideTests : RavenTestBase
{
    public AiAgentSchemaOverrideTests(ITestOutputHelper output) : base(output)
    {
    }

    private class DefaultSchema
    {
        public string Answer { get; set; }
    }

    private class AlternativeSchema
    {
        public string Summary { get; set; }
        public int Score { get; set; }
    }

    private static AiAgentConfiguration BuildSimpleAgent(string connectionStringName)
    {
        var agent = new AiAgentConfiguration("test assistant", connectionStringName,
            "You are a helpful assistant. Answer questions concisely. Always provide a relevant answer.");

        return agent;
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanOverrideSchemaWithSampleObject(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildSimpleAgent(config.ConnectionStringName);
        var r = await store.AI.CreateAgentAsync(agent, new DefaultSchema { Answer = "the answer" });

        var chat = store.AI.Conversation(r.Identifier, "chats/", new AiConversationCreationOptions());

        chat.SetUserPrompt("Rate how helpful you are on a scale of 1 to 10 and summarize what you do.");
        var run = await chat.RunAsync<AlternativeSchema>(new AlternativeSchema { Summary = "a short summary", Score = 5 });

        Assert.Equal(AiConversationResult.Done, run.Status);
        Assert.NotNull(run.Answer);
        Assert.NotNull(run.Answer.Summary);
        Assert.True(run.Answer.Score > 0, "Score should be a positive number");
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanOverrideSchemaWithJsonString(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildSimpleAgent(config.ConnectionStringName);
        var r = await store.AI.CreateAgentAsync(agent, new DefaultSchema { Answer = "the answer" });

        var chat = store.AI.Conversation(r.Identifier, "chats/", new AiConversationCreationOptions());

        string schemaJson = """
        {
            "name": "alternative_output",
            "strict": true,
            "schema": {
                "type": "object",
                "properties": {
                    "Summary": { "type": "string" },
                    "Score": { "type": "integer" }
                },
                "required": ["Summary", "Score"],
                "additionalProperties": false
            }
        }
        """;

        chat.SetUserPrompt("Rate how helpful you are on a scale of 1 to 10 and summarize what you do.");
        var run = await chat.RunAsync<AlternativeSchema>(new AiOutputOptions
        {
            OutputSchema = schemaJson
        });

        Assert.Equal(AiConversationResult.Done, run.Status);
        Assert.NotNull(run.Answer);
        Assert.NotNull(run.Answer.Summary);
        Assert.True(run.Answer.Score > 0, "Score should be a positive number");
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanRunWithoutSchema(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildSimpleAgent(config.ConnectionStringName);
        var r = await store.AI.CreateAgentAsync(agent, new DefaultSchema { Answer = "the answer" });

        var chat = store.AI.Conversation(r.Identifier, "chats/", new AiConversationCreationOptions());

        chat.SetUserPrompt("Say hello in one sentence.");
        var run = await chat.RunAsync<string>(new AiOutputOptions());

        Assert.Equal(AiConversationResult.Done, run.Status);
        Assert.NotNull(run.Answer);
        Assert.NotEmpty(run.Answer);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanRunStringWithoutExplicitOptions(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildSimpleAgent(config.ConnectionStringName);
        var r = await store.AI.CreateAgentAsync(agent, new DefaultSchema { Answer = "the answer" });

        var chat = store.AI.Conversation(r.Identifier, "chats/", new AiConversationCreationOptions());

        // RunAsync<string>() without options should auto-redirect to NoSchema
        chat.SetUserPrompt("Say hello in one sentence.");
        var run = await chat.RunAsync<string>();

        Assert.Equal(AiConversationResult.Done, run.Status);
        Assert.NotNull(run.Answer);
        Assert.NotEmpty(run.Answer);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanSwitchSchemasBetweenTurns(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildSimpleAgent(config.ConnectionStringName);
        var r = await store.AI.CreateAgentAsync(agent, new DefaultSchema { Answer = "the answer" });

        var chat = store.AI.Conversation(r.Identifier, "chats/", new AiConversationCreationOptions());

        // Turn 1: use agent default schema
        chat.SetUserPrompt("Say hello.");
        var run1 = await chat.RunAsync<DefaultSchema>();
        Assert.Equal(AiConversationResult.Done, run1.Status);
        Assert.NotNull(run1.Answer.Answer);

        // Turn 2: override with alternative schema
        chat.SetUserPrompt("Rate how helpful you are on a scale of 1 to 10 and summarize what you do.");
        var run2 = await chat.RunAsync<AlternativeSchema>(new AlternativeSchema { Summary = "a short summary", Score = 5 });
        Assert.Equal(AiConversationResult.Done, run2.Status);
        Assert.NotNull(run2.Answer.Summary);
        Assert.True(run2.Answer.Score > 0);

        // Turn 3: back to agent default
        chat.SetUserPrompt("What did I ask you first?");
        var run3 = await chat.RunAsync<DefaultSchema>();
        Assert.Equal(AiConversationResult.Done, run3.Status);
        Assert.NotNull(run3.Answer.Answer);

        // Turn 4: string output, no schema
        chat.SetUserPrompt("Say goodbye in one sentence.");
        var run4 = await chat.RunAsync<string>(new AiOutputOptions());
        Assert.Equal(AiConversationResult.Done, run4.Status);
        Assert.NotNull(run4.Answer);
        Assert.NotEmpty(run4.Answer);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanStreamWithSchemaOverride(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildSimpleAgent(config.ConnectionStringName);
        var r = await store.AI.CreateAgentAsync(agent, new DefaultSchema { Answer = "the answer" });

        var chat = store.AI.Conversation(r.Identifier, "chats/", new AiConversationCreationOptions());

        var streamed = new StringBuilder();
        chat.SetUserPrompt("Rate how helpful you are on a scale of 1 to 10 and summarize what you do.");
        var run = await chat.StreamAsync<AlternativeSchema>(
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

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanStreamStringWithoutSchema(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildSimpleAgent(config.ConnectionStringName);
        var r = await store.AI.CreateAgentAsync(agent, new DefaultSchema { Answer = "the answer" });

        var chat = store.AI.Conversation(r.Identifier, "chats/", new AiConversationCreationOptions());

        var streamed = new StringBuilder();
        chat.SetUserPrompt("Tell me a short joke in two sentences.");
        var run = await chat.StreamAsync(chunk =>
        {
            streamed.Append(chunk);
            return Task.CompletedTask;
        });

        Assert.Equal(AiConversationResult.Done, run.Status);
        Assert.NotNull(run.Answer);
        Assert.NotEmpty(run.Answer);
        Assert.Equal(streamed.ToString(), run.Answer);
        Assert.NotEmpty(streamed.ToString());
    }
}
