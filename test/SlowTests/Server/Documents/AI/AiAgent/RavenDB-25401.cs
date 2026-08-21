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
}
