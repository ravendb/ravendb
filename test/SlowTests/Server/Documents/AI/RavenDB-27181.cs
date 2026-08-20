using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Client.Documents.Conventions;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Server.Documents.AI;

public class RavenDB_27181(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string ReasoningEffortField = "\"reasoning_effort\":";

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "gpt-5.4" })]
    public async Task AgentWithTools_OnAffectedModel_Answers(Options options, GenAiConfiguration config, string model)
    {
        using var store = GetDocumentStore(options);

        config.Connection.OpenAiSettings.Model = model;
        config.Connection.OpenAiSettings.ReasoningEffort = "high";
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("reasoning-effort-with-tools", config.ConnectionStringName, SystemPrompt) { SampleObject = SampleAnswer };
        agent.Actions.Add(new AiAgentToolAction { Name = "GetUserAllergies", Description = "Get the allergies of the current user", ParametersSampleObject = "{}" });

        var traces = await RunConversationAsync(store, agent);

        Assert.All(traces, trace =>
        {
            Assert.Contains("\"tools\":", trace);
            Assert.Contains($"{ReasoningEffortField}\"none\"", trace);
        });
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "gpt-5.4" })]
    public async Task AgentWithoutTools_OnAffectedModel_AlsoRunsWithoutReasoning(Options options, GenAiConfiguration config, string model)
    {
        using var store = GetDocumentStore(options);

        config.Connection.OpenAiSettings.Model = model;
        config.Connection.OpenAiSettings.ReasoningEffort = "high";
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("reasoning-effort-no-tools", config.ConnectionStringName, SystemPrompt) { SampleObject = SampleAnswer };

        var traces = await RunConversationAsync(store, agent);

        Assert.All(traces, trace =>
        {
            Assert.DoesNotContain("\"tools\":", trace);
            Assert.Contains($"{ReasoningEffortField}\"none\"", trace);
        });
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { "gpt-5.2" })]
    public async Task AgentWithTools_OnUnaffectedModel_KeepsConfiguredReasoning(Options options, GenAiConfiguration config, string model)
    {
        using var store = GetDocumentStore(options);

        // the last family that accepts tools together with reasoning, so the policy must not reach it
        config.Connection.OpenAiSettings.Model = model;
        config.Connection.OpenAiSettings.ReasoningEffort = "high";
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("reasoning-effort-with-tools-unaffected", config.ConnectionStringName, SystemPrompt) { SampleObject = SampleAnswer };
        agent.Actions.Add(new AiAgentToolAction { Name = "GetUserAllergies", Description = "Get the allergies of the current user", ParametersSampleObject = "{}" });

        var traces = await RunConversationAsync(store, agent);

        Assert.All(traces, trace =>
        {
            Assert.Contains("\"tools\":", trace);
            Assert.Contains($"{ReasoningEffortField}\"high\"", trace);
        });
    }

    [RavenTheory(RavenTestCategory.Ai)]
    // "High" is also the shape persisted by earlier enum-based versions, which must keep round-tripping
    [InlineData("High")]
    [InlineData("xhigh")]
    [InlineData(null)]
    public void ReasoningEffortSurvivesAConnectionStringRoundTrip(string effort)
    {
        using var store = GetDocumentStore();

        var connectionString = new AiConnectionString
        {
            Name = "reasoning-effort-round-trip",
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings("api-key", Endpoint, "gpt-5-mini", reasoningEffort: effort)
        };

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));

        var connectionStrings = store.Maintenance.Send(new GetConnectionStringsOperation(connectionString.Name, ConnectionStringType.Ai)).AiConnectionStrings;
        var settings = connectionStrings[connectionString.Name].OpenAiSettings;

        Assert.Equal(effort, settings.ReasoningEffort);
    }

    [RavenFact(RavenTestCategory.Ai)]
#pragma warning disable CS0618 // reproduces what an enum-based client and server actually wrote
    public void LegacyEnumWasSerializedAsItsMemberName()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            // the connection string PUT path serializes the entity with DocumentConventions.Default,
            // which registers a StringEnumConverter because SaveEnumsAsIntegers defaults to false
            var onTheWire = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(
                new LegacySettingsShape { ReasoningEffort = OpenAiReasoningEffort.High }, context);

            Assert.True(onTheWire.TryGet(nameof(OpenAiSettings.ReasoningEffort), out string sentByOldClient));
            Assert.Equal("High", sentByOldClient);

            // the server persisted it through ToJson, where a boxed enum becomes its member name
            var persisted = context.ReadObject(new DynamicJsonValue
            {
                [nameof(OpenAiSettings.ReasoningEffort)] = (OpenAiReasoningEffort?)OpenAiReasoningEffort.High
            }, "old-persisted-settings");

            Assert.True(persisted.TryGet(nameof(OpenAiSettings.ReasoningEffort), out string storedByOldServer));
            Assert.Equal("High", storedByOldServer);
        }
    }
#pragma warning restore CS0618

    [RavenFact(RavenTestCategory.Ai)]
    public void NumericReasoningEffortDeserializesAsNotConfigured()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            // no supported client wrote this shape (the PUT path hardcodes DocumentConventions.Default,
            // where SaveEnumsAsIntegers is false), but a number must degrade rather than throw
            var json = context.ReadObject(new DynamicJsonValue
            {
                [nameof(OpenAiSettings.Model)] = "gpt-5-mini",
                [nameof(OpenAiSettings.ReasoningEffort)] = 3L
            }, "numeric-settings");

            var settings = JsonDeserializationServer.OpenAiSettings(json);

            Assert.Null(settings.ReasoningEffort);
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    // every value an enum-based client could have written, normalized to the provider form
    [InlineData("gpt-5-mini", "Minimal", "minimal")]
    [InlineData("gpt-5-mini", "Low", "low")]
    [InlineData("gpt-5-mini", "Medium", "medium")]
    [InlineData("gpt-5-mini", "High", "high")]
    // values a client that never had the enum can write, passed through untouched
    [InlineData("gpt-5.2", "xhigh", "xhigh")]
    [InlineData("gpt-5.2", "max", "max")]
    [InlineData("gpt-5-mini", "none", "none")]
    [InlineData("gpt-5-mini", "future-effort", "future-effort")]
    // the GPT-5.4+ workaround still wins over whatever was configured
    [InlineData("gpt-5.4", "High", "none")]
    public async Task LegacyConfigurationReachesTheProviderInTheExpectedForm(string model, string stored, string expected)
    {
        using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(),
            new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()));

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            // exactly what sits in the cluster for a connection string saved before this change
            var legacyJson = context.ReadObject(new DynamicJsonValue
            {
                [nameof(AiConnectionString.Name)] = "legacy-connection",
                [nameof(AiConnectionString.ModelType)] = AiModelType.Chat,
                [nameof(AiConnectionString.OpenAiSettings)] = new DynamicJsonValue
                {
                    [nameof(OpenAiSettings.ApiKey)] = "api-key",
                    [nameof(OpenAiSettings.Endpoint)] = Endpoint,
                    [nameof(OpenAiSettings.Model)] = model,
                    [nameof(OpenAiSettings.ReasoningEffort)] = stored
                }
            }, "legacy-connection-string");

            // the real server-side PUT/load deserializer reads the legacy shape into the string property
            var connectionString = JsonDeserializationCluster.AiConnectionString(legacyJson);
            Assert.Equal(stored, connectionString.OpenAiSettings.ReasoningEffort);

            using var client = ChatCompletionClient.CreateChatCompletionClient(contextPool, connectionString);

            using (var stream = new MemoryStream())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                client.WriteCompletionRequestPayload(writer, context, [], [], tools: null, useTools: true, streaming: false, ChatCompletionClient.EmptySchema);
                await writer.FlushAsync();

                Assert.Contains($"{ReasoningEffortField}\"{expected}\"", Encoding.UTF8.GetString(stream.ToArray()));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
#pragma warning disable CS0618 // proves the migration path for the obsolete enum
    [InlineData(OpenAiReasoningEffort.Minimal, "minimal")]
    [InlineData(OpenAiReasoningEffort.High, "high")]
    public void ToReasoningEffortLowercasesTheLegacyEnum(OpenAiReasoningEffort legacy, string expected)
    {
        Assert.Equal(expected, legacy.ToReasoningEffort());
    }
#pragma warning restore CS0618

    [RavenTheory(RavenTestCategory.Ai)]
    // gpt-5.4 and later never get reasoning, and the minor version is compared as a number, not as text
    [InlineData("gpt-5.4", "high", "none")]
    [InlineData("gpt-5.6-sol", "xhigh", "none")]
    [InlineData("gpt-5.10", "high", "none")]
    // a future major without a minor version deliberately follows the same policy
    [InlineData("gpt-6", "high", "none")]
    // gateways expose the model with a provider prefix, but a name that merely contains it is a different model
    [InlineData("openai/gpt-5.6-sol", "high", "none")]
    [InlineData("my-gpt-5.4-clone", "high", "high")]
    // earlier models keep the configured effort
    [InlineData("gpt-5.2", "high", "high")]
    [InlineData("gpt-5", "high", "high")]
    // OpenAI-compatible providers expose Azure-style names such as gpt-35-turbo, which is not version 35
    [InlineData("gpt-35-turbo", "high", "high")]
    // legacy member names are normalized wherever they come from, including a hand-migrated string
    [InlineData("gpt-5-mini", "High", "high")]
    [InlineData("gpt-5-mini", "Medium", "medium")]
    // numeric strings are provider values, not the numbers behind the legacy enum members
    [InlineData("gpt-5-mini", "3", "3")]
    [InlineData("gpt-5-mini", "42", "42")]
    // new values pass through as supplied apart from trimming, and matching is exact beyond the legacy names
    [InlineData("gpt-5.2", "xhigh", "xhigh")]
    [InlineData("gpt-5.2", "max", "max")]
    [InlineData("gpt-5-mini", " high ", "high")]
    [InlineData("gpt-5-mini", "HIGH", "HIGH")]
    [InlineData("gpt-5-mini", "   ", null)]
    [InlineData("gpt-5-mini", null, null)]
    public async Task EffectiveReasoningEffortIsWrittenToThePayload(string model, string effort, string expected)
    {
        var settings = new OpenAiSettings("api-key", Endpoint, model, reasoningEffort: effort);
        var connectionString = new AiConnectionString { Name = "test-connection", ModelType = AiModelType.Chat, OpenAiSettings = settings };

        using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(),
            new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()));
        using var client = ChatCompletionClient.CreateChatCompletionClient(contextPool, connectionString);

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var stream = new MemoryStream())
        await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
        {
            client.WriteCompletionRequestPayload(writer, context, [], [], tools: null, useTools: true, streaming: false, ChatCompletionClient.EmptySchema);
            await writer.FlushAsync();

            var payload = Encoding.UTF8.GetString(stream.ToArray());

            if (expected == null)
                Assert.DoesNotContain("reasoning_effort", payload);
            else
                Assert.Contains($"{ReasoningEffortField}\"{expected}\"", payload);
        }
    }

    private const string SystemPrompt = "You are a helpful assistant. Answer concisely.";
    private const string SampleAnswer = "{\"Answer\":\"answer here\"}";

    private static async Task<List<string>> RunConversationAsync(IDocumentStore store, AiAgentConfiguration agent)
    {
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/", creationOptions: null, debug: true);
        chat.SetUserPrompt("What is 2+2?");
        var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);

        Assert.Equal(AiConversationResult.Done, result.Status);

        using var session = store.OpenAsyncSession();
        var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();

        Assert.NotEmpty(traces);
        return traces.Select(x => x.RequestBody).ToList();
    }

    private class OutputSchema
    {
        public static readonly OutputSchema Instance = new();
        public string Answer = "Answer to the user question";
    }

    // the shape OpenAiSettings had before ReasoningEffort became a string
    private sealed class LegacySettingsShape
    {
#pragma warning disable CS0618
        public OpenAiReasoningEffort? ReasoningEffort { get; set; }
#pragma warning restore CS0618
    }

    private class DebugTraceDoc
    {
        public string RequestBody { get; set; }
    }

    private const string Endpoint = "https://api.openai.com/";
}
