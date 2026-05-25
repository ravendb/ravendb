using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
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
        Assert.Equal("hello",  result.Parameters["strParam"]);

        // Number
        Assert.Equal(3_000_000_000L, result.Parameters["numParam"]);
        Assert.Equal(0.5, result.Parameters["numParam2"]);

        // Boolean
        Assert.Equal(true, result.Parameters["boolParam"]);

        // ArrayOfString
        var strArr = Assert.IsType<List<string>>(result.Parameters["strArr"]);
        Assert.Equal(new[] { "a", "b", "c" }, strArr);

        // ArrayOfNumber
        var numArr = Assert.IsType<List<long>>(result.Parameters["numArr"]);
        Assert.Equal(new[] { 1_000_000_000_000L, 2_000_000_000_000L, 5L }, numArr);

        // ArrayOfNumber
        var numArr2 = Assert.IsType<List<double>>(result.Parameters["numArr2"]);
        Assert.Equal(new[] { 0.5 }, numArr2);

        // ArrayOfBoolean
        var boolArr = Assert.IsType<List<bool>>(result.Parameters["boolArr"]);
        Assert.Equal(new[] { true, false, true }, boolArr);

        // Null
        Assert.Null(result.Parameters["nullParam"]);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GetConversationMessages_ReturnsParameters_OldFormatNormalized()
    {
        // Old storage format: raw value (no wrapper object), e.g. {"budgetNis": 3500}
        // GetAiConversationParameter should normalize it to its raw Value
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

        Assert.Equal(3500L, result.Parameters["budgetNis"]);
        Assert.Equal("north", result.Parameters["region"]);
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

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GetConversationMessages_StitchesAcrossTwoSummarizationHistoryDocs(Options options, GenAiConfiguration config)
    {
        // Drives a conversation through TWO summarizations so two history snapshots get persisted.
        // Verifies the GetConversationMessages API stitches the full timeline back together
        // (messages from both history docs + the current doc, in chronological order).
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration(AgentName, config.ConnectionStringName, "You are a helpful assistant. Respond very briefly.");
        var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

        const string conversationId = "chats/two-summaries";
        var chat = store.AI.Conversation(agentId, conversationId, new AiConversationCreationOptions());

        // Turns 1 + 2: build baseline messages with no trimming.
        chat.SetUserPrompt("Hello, I'm Alice");
        await chat.RunAsync<OutputSchema>();

        chat.SetUserPrompt("What is the capital of France?");
        await chat.RunAsync<OutputSchema>();

        // Enable summarization (MaxTokensBeforeSummarization = 0 forces it on every subsequent turn)
        // and history persistence so each summarization snapshots the previous state into a history doc.
        agent.ChatTrimming = new AiAgentChatTrimmingConfiguration
        {
            Tokens = new AiAgentSummarizationByTokens { MaxTokensBeforeSummarization = 0 },
            History = new AiAgentHistoryConfiguration()
        };
        await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        // Turn 3: first forced summarization → 1st history doc
        chat.SetUserPrompt("Tell me a fun fact about Paris");
        await chat.RunAsync<OutputSchema>();

        // Turn 4: second forced summarization → 2nd history doc.
        chat.SetUserPrompt("What's the population there?");
        await chat.RunAsync<OutputSchema>();

        // Sanity-check the current doc has at least 2 linked history docs
        Chat chatDoc;
        using (var session = store.OpenAsyncSession())
            chatDoc = await session.LoadAsync<Chat>(conversationId);

        Assert.NotNull(chatDoc);
        Assert.True(chatDoc.LinkedConversations is { Count: >= 2 },
            $"Expected at least 2 history snapshots, got {chatDoc.LinkedConversations?.Count ?? 0}");

        // Fetch via the public API — should stitch current + 2 history docs into a single timeline.
        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Simple,
                PageSize = 200
            });

        // All 4 user prompts must appear in the stitched timeline — the first three live in
        // history docs after summarization, only the latest remains in the current doc.
        var userPrompts = result.Messages
            .Where(m => m.Role == AiMessageRole.User)
            .Select(m => m.Content ?? "")
            .ToList();

        Assert.Contains(userPrompts, c => c.Contains("Alice"));
        Assert.Contains(userPrompts, c => c.Contains("capital of France"));
        Assert.Contains(userPrompts, c => c.Contains("fun fact about Paris"));
        Assert.Contains(userPrompts, c => c.Contains("population"));

        // Timestamps must be monotonically non-decreasing (oldest first, per the DTO contract).
        for (int i = 1; i < result.Messages.Count; i++)
            Assert.True(result.Messages[i].Timestamp >= result.Messages[i - 1].Timestamp,
                $"Message {i} timestamp ({result.Messages[i].Timestamp:O}) is not >= message {i - 1} ({result.Messages[i - 1].Timestamp:O})");

        // Confirm stitching actually happened: more messages came back than what's in the current doc alone.
        int currentDocMessageCount = chatDoc.Messages?.Count ?? 0;
        Assert.True(result.Messages.Count > currentDocMessageCount,
            $"Expected stitched result ({result.Messages.Count}) to exceed current-doc message count ({currentDocMessageCount}).");
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanGetConversationMessages_ArrayContentJoinsTextParts_FromRealConversation(Options options, GenAiConfiguration config)
    {
        // End-to-end version of CanGetConversationMessages_ArrayContentJoinsTextParts:
        // instead of hand-crafting a conversation doc with an array-content user message,
        // we drive a real turn that calls SetUserPrompt + AddUserPrompt (which produces a
        // multi-part text content array in storage), plus AddAttachment (which produces the
        // synthetic "[Attachments: ...]" marker). Then verify the reader joins the parts and
        // parses the marker the same way the unit test does.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("image-analyzer", config.ConnectionStringName,
            "You are my friend have a chat with me");
        agent.Identifier = "image-analyzer";

        await store.AI.CreateAgentAsync(agent, new OutputSchema());

        var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());
        chat.SetUserPrompt("what are inside the images I sent you? what are their colors?");

        await using (var banana = GetEmbeddedImgStream("banana.png"))
        await using (var star = GetEmbeddedImgStream("star.png"))
        await using (var heart = GetEmbeddedImgStream("heart.png"))
        {
            chat.AddAttachment("banana.png", banana, "image/png");
            chat.AddAttachment("star.png", star, "image/png");
            chat.AddAttachment("heart.png", heart, "image/png");
            chat.AddUserPrompt("what do you see on those images?");

            var answer = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, answer.Status);
            Assert.NotNull(answer.Answer);
        }

        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = chat.Id,
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 50
            });

        // Locate the user message produced from the multi-part text array (SetUserPrompt + AddUserPrompt).
        // It must contain both prompts joined by Environment.NewLine — the same join behavior
        // CanGetConversationMessages_ArrayContentJoinsTextParts asserts on a hand-crafted doc.
        var expectedJoined =
            "what are inside the images I sent you? what are their colors?"
            + Environment.NewLine +
            "what do you see on those images?";

        var joinedMsg = result.Messages.SingleOrDefault(m =>
            m.Role == AiMessageRole.User && m.Content == expectedJoined);

        Assert.True(joinedMsg != null,
            "Expected a user message with the two prompts joined by a newline. Got contents: "
            + string.Join(" | ", result.Messages.Where(m => m.Role == AiMessageRole.User).Select(m => $"\"{m.Content}\"")));

        // The "[Attachments: ...]" synthetic message should produce a Content-null user message
        // whose Attachments field carries the three file names.
        var attachmentsMsg = Assert.Single(result.Messages, m =>
            m.Role == AiMessageRole.User && m.Attachments is { Count: 3 });
        Assert.Null(attachmentsMsg.Content);
        Assert.Equal(new[] { "banana.png", "star.png", "heart.png" }, attachmentsMsg.Attachments);

        // At least one assistant message with content (the model's answer) must come back.
        Assert.Contains(result.Messages, m =>
            m.Role == AiMessageRole.Assistant && string.IsNullOrEmpty(m.Content) == false);
    }

    // Minimal POCO for loading the persisted conversation document — we only need to inspect
    // LinkedConversations.Count and Messages.Count, so other fields are deliberately omitted.
    private class Chat
    {
        public List<object> Messages { get; set; }
        public List<string> LinkedConversations { get; set; }
    }

    private class OutputSchema
    {
        public static OutputSchema Instance = new() { Answer = "ok" };
        public string Answer { get; set; }
    }

    // Borrows the embedded test images from the RavenDB_24648 data set used by RavenDB_24847.
    private static Stream GetEmbeddedImgStream(string name)
    {
        var asm = typeof(RavenDB_24609).Assembly;
        var resourceName = "SlowTests.Data.RavenDB_24648." + name;

        var stream = asm.GetManifestResourceStream(resourceName);
        if (stream == null)
            throw new FileNotFoundException($"Embedded resource not found: {resourceName}");

        return stream;
    }
}
