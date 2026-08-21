using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;
public class RavenDB_25401 : RavenTestBase
{
    public RavenDB_25401(ITestOutputHelper output) : base(output)
    {
    }

    private const string SkipReason = "RavenDB-27400 - refusal-provoking prompts removed; coverage to be reworked on top of mocked provider responses";

    private class OutputSchema
    {
        public static readonly OutputSchema Instance = new()
        {
            Answer = "Answer to the user's question"
        };

        public string Answer { get; set; }
    }

    // Prompts removed under RavenDB-27400 - to be repopulated when this coverage moves to mocked provider responses.
    private static readonly string[] RefusalProvokingPrompts =
    [
    ];

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Skip = SkipReason)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.AzureOpenAI, DatabaseMode = RavenDatabaseMode.Single, Skip = SkipReason)]
    // [RavenGenAiData(IntegrationType = RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single, Skip = SkipReason)]
    public async Task AssertRefusalIsSurfacedDuringStreamingAsync(Options options, GenAiConfiguration config)
    {
        if (config.Connection.OpenAiSettings != null)
        {
            config.Connection.OpenAiSettings.Model = "gpt-4o";
            config.Connection.OpenAiSettings.ReasoningEffort = null;
        }
        
        if (config.Connection.GoogleSettings != null)
        {
            config.Connection.GoogleSettings.Model = "gemini-2.5-flash";
        }

        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("refusal-agent", config.ConnectionStringName,
            "You are a helpful assistant. Answer the user's question directly.");
        agent.Identifier = "refusal-agent";

        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var diagnostics = new StringBuilder();
        var refusalSurfaced = false;

        foreach (var prompt in RefusalProvokingPrompts)
        {
            var chat = store.AI.Conversation(createResult.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt(prompt);

            var streamed = new StringBuilder();
            try
            {
                var r = await chat.StreamAsync<OutputSchema>(
                    s => s.Answer,
                    chunk =>
                    {
                        streamed.Append(chunk);
                        return Task.CompletedTask;
                    },
                    CancellationToken.None);

                diagnostics
                    .Append("[NOT REFUSED] status=").Append(r.Status)
                    .Append(", answer='").Append(r.Answer?.Answer ?? "<null>").Append('\'')
                    .Append(", streamed='").Append(streamed).Append('\'')
                    .Append(", prompt=").AppendLine(prompt);
            }
            catch (AiException e) when (e.Message.Contains("RefusedToAnswerException"))
            {
                refusalSurfaced = true;
                break;
            }
        }

        Assert.True(refusalSurfaced,
            $"Expected at least one disallowed prompt to surface a {nameof(RefusedToAnswerException)} during streaming, " +
            $"but the refusal was silently swallowed (no refusal was checked on the streamed response). " +
            $"Provider: {config.Connection.GetActiveProvider()}.{Environment.NewLine}{diagnostics}");
    }

    private const string DummyConnectionStringName = "refusal-exception-type";

    private const string ExpectedRefusal = "I'm sorry, I can't help with that.";
    private const string ExpectedFinishReason = "content_filter";

    /// <summary>
    /// A refusal must reach the client as <see cref="RefusedToAnswerException"/>, with its <c>Refusal</c> and
    /// <c>FinishReason</c> intact. The server already carries both across the wire:
    /// <c>RavenServerStartup.MaybeAddAdditionalExceptionData</c> writes them into the error payload for a
    /// <see cref="RefusedToAnswerException"/>, and <c>ExceptionDispatcher.FillException</c> reads them back.
    /// <para>
    /// What defeats it is <c>AbstractAiAgentProcessor.ExecuteInternalAsync</c>: its rethrow allowlist does not include
    /// <see cref="RefusedToAnswerException"/>, so a refusal is wrapped in a generic <see cref="AiException"/>. The
    /// <c>case RefusedToAnswerException</c> then never matches, the two fields are never serialized, and the refusal
    /// survives only as text inside the wrapper's stack trace.
    /// </para>
    /// </summary>
    [RavenFact(RavenTestCategory.Ai)]
    public async Task RefusalShouldReachTheClientAsRefusedToAnswerException()
    {
        var (store, agentId) = await SetupAgentThatRefusesAsync();
        using (store)
        {
            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("hello");

            var e = await Assert.ThrowsAnyAsync<Exception>(() => chat.RunAsync<OutputSchema>(CancellationToken.None));

            AssertRefusalSurfaced(e);
        }
    }

    /// <inheritdoc cref="RefusalShouldReachTheClientAsRefusedToAnswerException"/>
    [RavenFact(RavenTestCategory.Ai)]
    public async Task RefusalShouldReachTheClientAsRefusedToAnswerExceptionWhenStreaming()
    {
        var (store, agentId) = await SetupAgentThatRefusesAsync();
        using (store)
        {
            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("hello");

            var e = await Assert.ThrowsAnyAsync<Exception>(() => chat.StreamAsync<OutputSchema>(
                s => s.Answer,
                _ => Task.CompletedTask,
                CancellationToken.None));

            AssertRefusalSurfaced(e);
        }
    }

    private static void AssertRefusalSurfaced(Exception e)
    {
        var refused = Assert.IsType<RefusedToAnswerException>(e);

        Assert.Equal(ExpectedRefusal, refused.Refusal);
        Assert.Equal(ExpectedFinishReason, refused.FinishReason);
    }

    private async Task<(Raven.Client.Documents.IDocumentStore Store, string AgentId)> SetupAgentThatRefusesAsync()
    {
        var store = GetDocumentStore();

        // A dummy connection string is enough: BeforeAiAgentTalk throws before a completion request is ever built,
        // so nothing reaches a provider and this test needs no credentials.
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = DummyConnectionStringName,
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings(apiKey: "sk-test-dummy", endpoint: "https://api.openai.com/", model: "gpt-4o")
        }));

        var agent = new AiAgentConfiguration("refusal-exception-type-agent", DummyConnectionStringName,
            "You are a helpful assistant. Answer the user's question directly.");

        var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        // Stands in for ChatCompletionClient raising a refusal: same call site inside TalkAsync, so it propagates out
        // of HandleRequestAsync/HandleStreamingRequestAsync into AbstractAiAgentProcessor exactly as a real one does.
        database.ForTestingPurposesOnly().BeforeAiAgentTalk = _ =>
            RefusedToAnswerException.Throw(ExpectedRefusal, responseContent: "{}", ExpectedFinishReason, requestId: "req-refusal-123");

        return (store, agentId);
    }
}
