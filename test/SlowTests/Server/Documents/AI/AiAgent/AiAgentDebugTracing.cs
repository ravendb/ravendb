using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using AiTestData = SlowTests.Server.Documents.AI.GenAi.Data;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class AiAgentDebugTracing : RavenTestBase
{
    public AiAgentDebugTracing(ITestOutputHelper output) : base(output)
    {
    }

    private class OutputSchema
    {
        public static readonly OutputSchema Instance = new();
        public string Answer = "Answer to the user question";
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task DebugTracesCreatedWhenEnabled(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Answer concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/",
            creationOptions: null, enableFullDebug: true);
        chat.SetUserPrompt("What is 2+2?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        Assert.True(stats.Collections.TryGetValue(Constants.Documents.Collections.AiAgentConversationDebugCollection, out long count));
        Assert.True(count == 1);

        using var session = store.OpenAsyncSession();
        var tracesList = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.True(tracesList.Count == 1);

        var first = tracesList.First();
        Assert.NotNull(first.RequestBody);
        Assert.NotNull(first.Response);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task NoDebugTracesWhenNotEnabled(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Answer concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/", creationOptions: null);
        chat.SetUserPrompt("What is 2+2?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        Assert.False(stats.Collections.ContainsKey(Constants.Documents.Collections.AiAgentConversationDebugCollection));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnableFullDebug_FlippedOnExistingConversation_TracesOnlyNextTurn(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Answer concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        // Turn 1: debug NOT enabled — no traces should be written.
        var chat = store.AI.Conversation(createResult.Identifier, "chats/", creationOptions: null);
        chat.SetUserPrompt("What is 2+2?");
        await chat.RunAsync<OutputSchema>(CancellationToken.None);

        var statsAfterTurn1 = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        Assert.False(statsAfterTurn1.Collections.ContainsKey(Constants.Documents.Collections.AiAgentConversationDebugCollection));

        // Flip EnableFullDebug by patching the conversation document directly —
        // this is what a user would do in Studio to enable tracing mid-conversation.
        await store.Operations.SendAsync(new PatchOperation(
            chat.Id,
            changeVector: null,
            patch: new PatchRequest { Script = "this.EnableFullDebug = true;" },
            patchIfMissing: null));

        // Turn 2: same conversation, EnableFullDebug now persisted on the document.
        // A fresh handle with no change vector is used so the server skips the CV check
        // (the patch above changed the document's CV).
        var chat2 = store.AI.Conversation(createResult.Identifier, chat.Id, creationOptions: null, changeVector: null);
        chat2.SetUserPrompt("What is 3+3?");
        await chat2.RunAsync<OutputSchema>(CancellationToken.None);

        var statsAfterTurn2 = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        Assert.True(statsAfterTurn2.Collections.TryGetValue(Constants.Documents.Collections.AiAgentConversationDebugCollection, out long count));
        Assert.True(count == 1);

        using var session = store.OpenAsyncSession();
        var tracesList = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.True(tracesList.Count == 1);
        Assert.NotNull(tracesList.First().RequestBody);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnableFullDebug_QueryStringOverride_FlipsPersistedAttributeFromFalseToTrue(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Answer concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        // Turn 1: no override on a brand-new conversation — should persist as false (default).
        var chat = store.AI.Conversation(createResult.Identifier, "chats/", creationOptions: null);
        chat.SetUserPrompt("What is 2+2?");
        await chat.RunAsync<OutputSchema>(CancellationToken.None);

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<ConversationDocShape>(chat.Id);
            Assert.NotNull(doc);
            Assert.False(doc.EnableFullDebug, "Expected EnableFullDebug=false persisted on a fresh conversation with no override");
        }

        // Turn 2: same conversation, ?enableFullDebug=true — must flip persisted false → true
        // AND must take effect on this turn (traces are written).
        var chat2 = store.AI.Conversation(createResult.Identifier, chat.Id,
            creationOptions: null, changeVector: null, enableFullDebug: true);
        chat2.SetUserPrompt("What is 3+3?");
        await chat2.RunAsync<OutputSchema>(CancellationToken.None);

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<ConversationDocShape>(chat.Id);
            Assert.NotNull(doc);
            Assert.True(doc.EnableFullDebug, "Expected EnableFullDebug=true persisted after ?enableFullDebug=true");

            var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
            Assert.True(traces.Count == 1, "Expected at least one trace document after the turn that flipped the flag on");
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnableFullDebug_QueryStringOverride_FlipsPersistedAttributeFromTrueToFalse(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Answer concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        // Turn 1: ?enableFullDebug=true on a brand-new conversation — must persist as true.
        var chat = store.AI.Conversation(createResult.Identifier, "chats/",
            creationOptions: null, enableFullDebug: true);
        chat.SetUserPrompt("What is 2+2?");
        await chat.RunAsync<OutputSchema>(CancellationToken.None);

        int tracesAfterTurn1;
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<ConversationDocShape>(chat.Id);
            Assert.NotNull(doc);
            Assert.True(doc.EnableFullDebug, "Expected EnableFullDebug=true persisted after ?enableFullDebug=true");

            tracesAfterTurn1 = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).Count();
            Assert.True(tracesAfterTurn1 == 1);
        }

        // Turn 2: same conversation, ?enableFullDebug=false — must flip persisted true → false
        // AND the override must be honored on this turn (no new traces written).
        var chat2 = store.AI.Conversation(createResult.Identifier, chat.Id,
            creationOptions: null, changeVector: null, enableFullDebug: false);
        chat2.SetUserPrompt("What is 3+3?");
        await chat2.RunAsync<OutputSchema>(CancellationToken.None);

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<ConversationDocShape>(chat.Id);
            Assert.NotNull(doc);
            Assert.False(doc.EnableFullDebug, "Expected EnableFullDebug=false persisted after ?enableFullDebug=false");

            var tracesAfterTurn2 = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).Count();
            Assert.Equal(tracesAfterTurn1, tracesAfterTurn2);
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnableFullDebug_WithAttachment_PersistsRequestWithAttachmentPayload(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Describe images concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/",
            creationOptions: null, enableFullDebug: true);
        chat.AddAttachment("heart.png", new MemoryStream(Convert.FromBase64String(AiTestData.HeartPngBase64)), "image/png");
        chat.SetUserPrompt("What do you see in this image?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using var session = store.OpenAsyncSession();
        var tracesList = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.True(tracesList.Count == 1);

        // The request payload must be present — it carries the base64-encoded image inline.
        Assert.NotNull(tracesList.First().RequestBody);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnableFullDebug_Streaming_PersistsStreamEvents(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Answer concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/",
            creationOptions: null, enableFullDebug: true);
        chat.SetUserPrompt("What is 2+2?");

        var streamedChunks = new List<string>();
        var r = await chat.StreamAsync<OutputSchema>(
            nameof(OutputSchema.Answer),
            chunk =>
            {
                streamedChunks.Add(chunk);
                return Task.CompletedTask;
            },
            CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.True(streamedChunks.Count > 0, "Expected at least one streamed chunk from the provider");

        using var session = store.OpenAsyncSession();
        var tracesList = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.True(tracesList.Count == 1);

        var first = tracesList.First();
        Assert.NotNull(first.RequestBody);
        Assert.Null(first.Response);        // streaming populates StreamEvents, not Response
        Assert.NotNull(first.StreamEvents); // at least one SSE event was captured
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnableFullDebug_WithQueryTool_PersistsOneTracePerLlmCall(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new { Name = "Widget A", Price = 9.99 }, "Products/1");
            await session.StoreAsync(new { Name = "Widget B", Price = 19.99 }, "Products/2");
            await session.SaveChangesAsync();
        }

        var agent = new AiAgentConfiguration("debug-query-agent", config.ConnectionStringName,
            "You are a helpful product assistant. Use the SearchProducts tool whenever the user asks about available products.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}",
            Queries =
            [
                new AiAgentToolQuery("SearchProducts", "Search for available products", "from Products")
                {
                    ParametersSampleObject = "{}"
                }
            ]
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/",
            creationOptions: null, enableFullDebug: true);
        chat.SetUserPrompt("What products do you have available?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using var session2 = store.OpenAsyncSession();
        var tracesList = (await session2.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();

        // At least 2 LLM calls: one that returned a tool call, one with the final answer.
        Assert.True(tracesList.Count == 2, $"Expected at least 2 trace documents, got {tracesList.Count}");
        Assert.All(tracesList, t => Assert.NotNull(t.RequestBody));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnableFullDebug_WithActionTool_PersistsOneTracePerLlmCall(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-action-agent", config.ConnectionStringName,
            "You are a helpful pricing assistant. Use the GetPrice tool to look up the price of any item the user asks about.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}",
            Actions =
            [
                new AiAgentToolAction
                {
                    Name = "GetPrice",
                    Description = "Get the current price of an item",
                    ParametersSampleObject = "{\"item\":\"item name\"}"
                }
            ]
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/",
            creationOptions: null, enableFullDebug: true);
        chat.Handle<object>("GetPrice", _ => "the price is $9.99");
        chat.SetUserPrompt("What is the price of a widget?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using var session = store.OpenAsyncSession();
        var tracesList = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();

        // At least 2 LLM calls: one that requested the action tool, one with the final answer.
        Assert.True(tracesList.Count == 2, $"Expected at least 2 trace documents, got {tracesList.Count}");
        Assert.All(tracesList, t => Assert.NotNull(t.RequestBody));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task EnableFullDebug_FailedLlmCall_TracesStillPersisted()
    {
        using var store = GetDocumentStore();

        var agentConfig = new AiAgentConfiguration("debug-fail-agent", "fake-connection",
            "You are a helpful assistant.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        string conversationDocId = null;

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = new ExposableMockLlmHandler(Server.ServerStore, database,
                onRequest: _ => new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent("{\"error\":{\"message\":\"We could not parse the JSON body of your request.\",\"type\":\"invalid_request_error\",\"param\":null,\"code\":null}}")
                })
            {
                Authentication = null
            };

            handler.Initialize(agentConfig, "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "What is 2+2?"
            }, changeVector: null, enableFullDebugOverride: true);

            await Assert.ThrowsAnyAsync<Exception>(() => handler.HandleRequest(context, CancellationToken.None));
            conversationDocId = handler.ConversationDocId;
        }

        Assert.NotNull(conversationDocId);

        // Despite the LLM failure the finally block must have persisted whatever traces were captured.
        var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        Assert.True(stats.Collections.TryGetValue(Constants.Documents.Collections.AiAgentConversationDebugCollection, out long count),
            "Expected the debug-trace collection to exist after a failed turn with EnableFullDebug=true");
        Assert.True(count == 1);

        using var session = store.OpenAsyncSession();
        var tracesList = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{conversationDocId}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.True(tracesList.Count == 1, $"Expected at least one trace document under '{conversationDocId}/{AiDebugTrace.TraceSegment}/'");
        Assert.NotNull(tracesList.First().RequestBody);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task EnableFullDebug_RequestSerializationThrowsMidWrite_PartialRequestBodyPersisted()
    {
        using var store = GetDocumentStore();

        var agentConfig = new AiAgentConfiguration("debug-partial-agent", "fake-connection",
            "You are a helpful assistant.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        string conversationDocId = null;

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            // Inject a ModifyPayload that writes some bytes then throws. The `finally` block in
            // CreateCompletionRequest must still capture whatever made it to the buffer
            // (RequestBody is the raw bytes, always — no parse is attempted).
            var handler = new ExposableMockLlmHandler(Server.ServerStore, database,
                onRequest: null,
                configureClient: client =>
                {
                    client.ForTestingPurposesOnly().ModifyPayload = w =>
                    {
                        w.WriteStartObject();
                        w.WritePropertyName("model");
                        w.WriteString("partial-mock");
                        throw new InvalidOperationException("simulated mid-write failure");
                    };
                })
            {
                Authentication = null
            };

            handler.Initialize(agentConfig, "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "What is 2+2?"
            }, changeVector: null, enableFullDebugOverride: true);

            await Assert.ThrowsAnyAsync<Exception>(() => handler.HandleRequest(context, CancellationToken.None));
            conversationDocId = handler.ConversationDocId;
        }

        Assert.NotNull(conversationDocId);

        var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        Assert.True(stats.Collections.TryGetValue(Constants.Documents.Collections.AiAgentConversationDebugCollection, out long count),
            "Expected the debug-trace collection to exist after a mid-write failure with EnableFullDebug=true");
        Assert.True(count == 1);

        using var session = store.OpenAsyncSession();
        var tracesList = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{conversationDocId}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.True(tracesList.Count == 1);

        var body = tracesList.First().RequestBody;
        Assert.NotNull(body);
        // The partial bytes that were written before the throw must be present — raw, possibly invalid JSON.
        Assert.Contains("partial-mock", body);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnableFullDebug_PropagatesToSubAgent(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var subAgent = new AiAgentConfiguration("debug-sub-agent", config.ConnectionStringName,
            "You answer arithmetic questions. Return a short numeric answer.")
        {
            SampleObject = "{\"Answer\":\"42\"}"
        };
        var subAgentResult = await store.AI.CreateAgentAsync(subAgent, OutputSchema.Instance);

        var parentAgent = new AiAgentConfiguration("debug-parent-agent", config.ConnectionStringName,
            "Delegate any arithmetic question to the math sub-agent and return its answer.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}",
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = subAgentResult.Identifier,
                    Description = "Use to answer arithmetic questions."
                }
            ]
        };
        var parentResult = await store.AI.CreateAgentAsync(parentAgent, OutputSchema.Instance);

        var chat = store.AI.Conversation(parentResult.Identifier, "chats/",
            creationOptions: null, enableFullDebug: true);
        chat.SetUserPrompt("What is 2+2?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        // Traces should exist for the parent conversation AND for at least one sub-agent conversation.
        var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        Assert.True(stats.Collections.TryGetValue(Constants.Documents.Collections.AiAgentConversationDebugCollection, out long count));
        Assert.True(count >= 2, $"Expected at least 2 trace documents (parent + sub-agent), got {count}");

        using var session = store.OpenAsyncSession();
        var parentTraces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.True(parentTraces.Count >= 1, "Expected at least one trace under the parent conversation prefix.");

        var allTraces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/")).ToList();
        // Sub-agent conversations have IDs of the form `{parent.Id}/{paramsHash}` and their traces
        // are nested under `{parent.Id}/{paramsHash}/{TraceSegment}/...`.
        var subAgentTraces = allTraces.Count - parentTraces.Count;
        Assert.True(subAgentTraces >= 1, $"Expected at least one trace under a sub-agent conversation prefix; got {subAgentTraces}.");
    }

    // Subclass that exposes the conversation document ID so the test can load the trace docs by prefix.
    // Also allows the test to configure the MockLlm right after it is created (e.g. to inject
    // a ModifyPayload hook that fails serialization mid-write).
    private class ExposableMockLlmHandler(
        Raven.Server.ServerWide.ServerStore server,
        DocumentDatabase database,
        Func<JObject, HttpResponseMessage> onRequest = null,
        Action<MockLlm> configureClient = null)
        : MockLlmConversationHandler(server, database, onRequest)
    {
        public string ConversationDocId => _document?.Id;

        protected internal override ChatCompletionClient CreateClient()
        {
            var client = (MockLlm)base.CreateClient();
            configureClient?.Invoke(client);
            return client;
        }
    }

    private class DebugTraceDoc
    {
        // Raw request body as written to the wire. Always a string — no parse is attempted server-side.
        public string RequestBody { get; set; }
        public object Response { get; set; }
        public object StreamEvents { get; set; }
    }

    private class ConversationDocShape
    {
        public bool EnableFullDebug { get; set; }
    }
}
