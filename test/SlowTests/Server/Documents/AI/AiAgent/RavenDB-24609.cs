using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_24609(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string AgentName = "params-test-agent";

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GetConversationMessages_ReturnsParameters(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration(AgentName, config.ConnectionStringName, "You are a helpful assistant. Respond briefly.");
        agent.Parameters.Add(new AiAgentParameter("strParam")   { Type = AiAgentParameterValueType.String });
        agent.Parameters.Add(new AiAgentParameter("numParam")   { Type = AiAgentParameterValueType.Number });
        agent.Parameters.Add(new AiAgentParameter("boolParam")  { Type = AiAgentParameterValueType.Boolean });
        agent.Parameters.Add(new AiAgentParameter("strArr")     { Type = AiAgentParameterValueType.ArrayOfString });
        agent.Parameters.Add(new AiAgentParameter("numArr")     { Type = AiAgentParameterValueType.ArrayOfNumber });
        agent.Parameters.Add(new AiAgentParameter("boolArr")    { Type = AiAgentParameterValueType.ArrayOfBoolean });
        agent.Parameters.Add(new AiAgentParameter("nullParam")  { Type = AiAgentParameterValueType.Null });
        var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(
            agentId,
            "chats/params-test",
            new AiConversationCreationOptions()
                .AddParameter("strParam",  "hello")
                .AddParameter("numParam",  3_000_000_000L) // requires long (> int.MaxValue)
                .AddParameter("numParam2",  0.5) // additional
                .AddParameter("boolParam", true)
                .AddParameter("strArr",    new[] { "a", "b", "c" })
                .AddParameter("numArr",    new[] { 1_000_000_000_000L, 2_000_000_000_000L, 5L })
                .AddParameter("numArr2",    new[] { 0.5 }) // additional
                .AddParameter("boolArr",   new[] { true, false, true })
                .AddParameter("nullParam", (object)null));
        chat.SetUserPrompt("Hello");
        await chat.RunAsync<OutputSchema>();

        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions { ConversationId = "chats/params-test", PageSize = 50 });

        Assert.NotNull(result.Parameters);
        Assert.Equal(9, result.Parameters.Count);

        // String
        Assert.Equal("hello",  result.Parameters["strParam"].Value);
        Assert.True(result.Parameters["strParam"].SendToModel);

        // Number
        Assert.Equal(3_000_000_000L, result.Parameters["numParam"].Value);
        Assert.True(result.Parameters["numParam"].SendToModel);

        Assert.Equal(0.5, result.Parameters["numParam2"].Value);
        Assert.True(result.Parameters["numParam2"].SendToModel);

        // Boolean
        Assert.Equal(true, result.Parameters["boolParam"].Value);
        Assert.True(result.Parameters["boolParam"].SendToModel);

        // ArrayOfString
        var strArr = Assert.IsType<List<string>>(result.Parameters["strArr"].Value);
        Assert.Equal(new[] { "a", "b", "c" }, strArr);
        Assert.True(result.Parameters["strArr"].SendToModel);

        // ArrayOfNumber
        var numArr = Assert.IsType<List<long>>(result.Parameters["numArr"].Value);
        Assert.Equal(new[] { 1_000_000_000_000L, 2_000_000_000_000L, 5L }, numArr);
        Assert.True(result.Parameters["numArr"].SendToModel);

        // ArrayOfNumber
        var numArr2 = Assert.IsType<List<double>>(result.Parameters["numArr2"].Value);
        Assert.Equal(new[] { 0.5 }, numArr2);
        Assert.True(result.Parameters["numArr2"].SendToModel);

        // ArrayOfBoolean
        var boolArr = Assert.IsType<List<bool>>(result.Parameters["boolArr"].Value);
        Assert.Equal(new[] { true, false, true }, boolArr);
        Assert.True(result.Parameters["boolArr"].SendToModel);

        // Null
        Assert.Null(result.Parameters["nullParam"].Value);
        Assert.True(result.Parameters["nullParam"].SendToModel);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GetConversationMessages_ReturnsParameters_SendToModelFalsePreserved(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration(AgentName, config.ConnectionStringName, "You are a helpful assistant. Respond briefly.");
        var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(
            agentId,
            "chats/params-sendtomodel",
            new AiConversationCreationOptions()
                .AddParameter("publicParam",   "visible", new AiConversationParameterOptions { SendToModel = true })
                .AddParameter("internalParam", "hidden",  new AiConversationParameterOptions { SendToModel = false }));
        chat.SetUserPrompt("Hello");
        await chat.RunAsync<OutputSchema>();

        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions { ConversationId = "chats/params-sendtomodel", PageSize = 50 });

        Assert.NotNull(result.Parameters);
        Assert.True(result.Parameters["publicParam"].SendToModel);
        Assert.False(result.Parameters["internalParam"].SendToModel);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GetConversationMessages_ReturnsParameters_OldFormatNormalized()
    {
        // Old storage format: raw value (no wrapper object), e.g. {"budgetNis": 3500}
        // GetAiConversationParameter should normalize it to {Value: 3500, SendToModel: true}
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            var doc = context.ReadObject(new DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = new DynamicJsonValue
                {
                    ["budgetNis"] = 3500L,    // old format: raw long
                    ["region"] = "north"       // old format: raw string
                },
                ["Messages"] = new DynamicJsonArray
                {
                    new DynamicJsonValue { ["role"] = "user", ["content"] = "hi", ["date"] = DateTime.UtcNow }
                },
                ["LinkedConversations"] = new DynamicJsonArray(),
                ["TotalUsage"] = new DynamicJsonValue
                    { ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 },
                ["OpenActionCalls"] = new DynamicJsonValue(),
                ["LastMessageAt"] = DateTime.UtcNow,
                ["CreatedAt"] = DateTime.UtcNow,
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection
                }
            }, "test-doc");

            database.DocumentsStorage.Put(context, "chats/old-format", null, doc);
            tx.Commit();
        }

        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions { ConversationId = "chats/old-format", PageSize = 50 });

        Assert.NotNull(result.Parameters);
        Assert.Equal(2, result.Parameters.Count);

        Assert.Equal(3500L, result.Parameters["budgetNis"].Value);
        Assert.True(result.Parameters["budgetNis"].SendToModel); // defaults to true for old format

        Assert.Equal("north", result.Parameters["region"].Value);
        Assert.True(result.Parameters["region"].SendToModel);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GetConversationMessages_ReturnsParameters_NoParametersReturnsEmptyObject(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration(AgentName, config.ConnectionStringName, "You are a helpful assistant. Respond briefly.");
        var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(agentId, "chats/no-params", new AiConversationCreationOptions());
        chat.SetUserPrompt("Hello");
        await chat.RunAsync<OutputSchema>();

        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions { ConversationId = "chats/no-params", PageSize = 50 });

        Assert.NotNull(result.Parameters);
        Assert.Empty(result.Parameters);
    }

    private class OutputSchema
    {
        public static OutputSchema Instance = new() { Answer = "ok" };
        public string Answer { get; set; }
    }
}
