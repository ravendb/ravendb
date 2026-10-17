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
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
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
            creationOptions: null, debug: true);
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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_FlippedOnExistingConversation_TracesOnlyNextTurn(Options options, GenAiConfiguration config)
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

        // Flip Debug by patching the conversation document directly —
        // this is what a user would do in Studio to enable tracing mid-conversation.
        await store.Operations.SendAsync(new PatchOperation(
            chat.Id,
            changeVector: null,
            patch: new PatchRequest { Script = "this.Debug = true;" },
            patchIfMissing: null));

        // Turn 2: same conversation, Debug now persisted on the document.
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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_QueryStringOverride_FlipsPersistedAttributeFromFalseToTrue(Options options, GenAiConfiguration config)
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
            Assert.False(doc.Debug, "Expected Debug=false persisted on a fresh conversation with no override");
        }

        // Turn 2: same conversation, ?debug=true — must flip persisted false → true
        // AND must take effect on this turn (traces are written).
        var chat2 = store.AI.Conversation(createResult.Identifier, chat.Id,
            creationOptions: null, changeVector: null, debug: true);
        chat2.SetUserPrompt("What is 3+3?");
        await chat2.RunAsync<OutputSchema>(CancellationToken.None);

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<ConversationDocShape>(chat.Id);
            Assert.NotNull(doc);
            Assert.True(doc.Debug, "Expected Debug=true persisted after ?debug=true");

            var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
            Assert.True(traces.Count == 1, "Expected at least one trace document after the turn that flipped the flag on");
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_QueryStringOverride_FlipsPersistedAttributeFromTrueToFalse(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Answer concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        // Turn 1: ?debug=true on a brand-new conversation — must persist as true.
        var chat = store.AI.Conversation(createResult.Identifier, "chats/",
            creationOptions: null, debug: true);
        chat.SetUserPrompt("What is 2+2?");
        await chat.RunAsync<OutputSchema>(CancellationToken.None);

        int tracesAfterTurn1;
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<ConversationDocShape>(chat.Id);
            Assert.NotNull(doc);
            Assert.True(doc.Debug, "Expected Debug=true persisted after ?debug=true");

            tracesAfterTurn1 = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).Count();
            Assert.True(tracesAfterTurn1 == 1);
        }

        // Turn 2: same conversation, ?debug=false — must flip persisted true → false
        // AND the override must be honored on this turn (no new traces written).
        var chat2 = store.AI.Conversation(createResult.Identifier, chat.Id,
            creationOptions: null, changeVector: null, debug: false);
        chat2.SetUserPrompt("What is 3+3?");
        await chat2.RunAsync<OutputSchema>(CancellationToken.None);

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<ConversationDocShape>(chat.Id);
            Assert.NotNull(doc);
            Assert.False(doc.Debug, "Expected Debug=false persisted after ?debug=false");

            var tracesAfterTurn2 = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).Count();
            Assert.Equal(tracesAfterTurn1, tracesAfterTurn2);
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_WithAttachment_PersistsAttachmentNames(Options options, GenAiConfiguration config)
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
            creationOptions: null, debug: true);
        chat.AddAttachment("heart.png", new MemoryStream(Convert.FromBase64String(AiTestData.HeartPngBase64)), "image/png");
        chat.SetUserPrompt("What do you see in this image?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using var session = store.OpenAsyncSession();
        var tracesList = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.True(tracesList.Count == 1);

        var first = tracesList.First();
        Assert.NotNull(first.RequestBody);

        // Attachment names are persisted as a separate field on the trace doc.
        Assert.NotNull(first.AttachmentNames);
        Assert.Contains("heart.png", first.AttachmentNames);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_Streaming_PersistsStreamEvents(Options options, GenAiConfiguration config)
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
            creationOptions: null, debug: true);
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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_WithQueryTool_PersistsOneTracePerLlmCall(Options options, GenAiConfiguration config)
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
            creationOptions: null, debug: true);
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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_WithActionTool_PersistsOneTracePerLlmCall(Options options, GenAiConfiguration config)
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
            creationOptions: null, debug: true);
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
    public async Task Debug_FailedLlmCall_TracesStillPersisted()
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
            }, changeVector: null, debugOverride: true);

            await Assert.ThrowsAnyAsync<Exception>(() => handler.HandleRequestAsync(context, CancellationToken.None));
            conversationDocId = handler.ConversationDocId;
        }

        Assert.NotNull(conversationDocId);

        // Despite the LLM failure the finally block must have persisted whatever traces were captured.
        var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        Assert.True(stats.Collections.TryGetValue(Constants.Documents.Collections.AiAgentConversationDebugCollection, out long count),
            "Expected the debug-trace collection to exist after a failed turn with Debug=true");
        Assert.True(count == 1);

        using var session = store.OpenAsyncSession();
        var tracesList = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{conversationDocId}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.True(tracesList.Count == 1, $"Expected at least one trace document under '{conversationDocId}/{AiDebugTrace.TraceSegment}/'");
        Assert.NotNull(tracesList.First().RequestBody);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Debug_RequestSerializationThrowsMidWrite_PartialRequestBodyPersisted()
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
            }, changeVector: null, debugOverride: true);

            await Assert.ThrowsAnyAsync<Exception>(() => handler.HandleRequestAsync(context, CancellationToken.None));
            conversationDocId = handler.ConversationDocId;
        }

        Assert.NotNull(conversationDocId);

        var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        Assert.True(stats.Collections.TryGetValue(Constants.Documents.Collections.AiAgentConversationDebugCollection, out long count),
            "Expected the debug-trace collection to exist after a mid-write failure with Debug=true");
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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { false })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { null })]
    public async Task Debug_PropagatesToSubAgent(Options options, GenAiConfiguration config, bool? debug)
    {
        using var store = await CreateStoreWithSubAgentArithmetic(options, config);

        const string conversationId = "chats/propagation-test";

        // Null case: we need the parent doc to already have Debug=true persisted so the
        // null override actually has something to preserve. Drive a non-arithmetic warm-up turn
        // that won't invoke the sub-agent, then the real turn below uses the null override.
        if (debug == null)
        {
            var warmup = store.AI.Conversation(_parentAgentIdentifier, conversationId,
                creationOptions: null, debug: true);
            warmup.SetUserPrompt("Just say hi. Do not call any tools.");
            await warmup.RunAsync<OutputSchema>(CancellationToken.None);

            // Reliability gate: if a future system-prompt change causes the parent to eagerly invoke
            // the sub-agent on a greeting, this assertion fires with a clear message — and the
            // real turn below would no longer be the first arithmetic prompt in the conversation.
            using var s = store.OpenAsyncSession();
            var warmupDoc = await s.LoadAsync<ConversationDocShape>(conversationId);
            Assert.NotNull(warmupDoc);
            Assert.True(warmupDoc.Debug, "warm-up should persist Debug=true");
            Assert.Empty(warmupDoc.SubConversationIds);
        }

        var chat = store.AI.Conversation(_parentAgentIdentifier, conversationId,
            creationOptions: null, changeVector: null, debug: debug);
        chat.SetUserPrompt("What is 2+2?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        var snap = await LoadDebugSnapshot(store, conversationId);
        Assert.NotNull(snap.SubId);

        // true OR null-after-true-warmup -> tracing on; false -> tracing off.
        var expectTracing = debug != false;
        Assert.Equal(expectTracing, snap.ParentPersisted);
        Assert.Equal(expectTracing, snap.SubPersisted);

        if (expectTracing)
        {
            // Lower bounds derived from the protocol, not LLM behavior:
            //   * sub-agent: 1 LLM call (no further tools defined on it).
            //   * parent: 2 LLM calls per arithmetic turn — one to receive the tool dispatch, one to
            //     finalize the answer after the sub-agent returned. The null case adds 1 more from
            //     the warm-up turn ("just say hi" -> single LLM call, no tool dispatch).
            // A real LLM may occasionally do an extra reflection / re-dispatch round, so we use
            // lower bounds rather than equality.
            var minParentTraces = debug == null ? 3 : 2;
            Assert.True(snap.ParentTraces >= minParentTraces,
                $"expected >= {minParentTraces} parent trace docs, got {snap.ParentTraces}");
            Assert.True(snap.SubAgentTraces >= 1,
                $"expected >= 1 sub-agent trace doc, got {snap.SubAgentTraces}");
        }
        else
        {
            Assert.Equal(0, snap.ParentTraces);
            Assert.Equal(0, snap.SubAgentTraces);

            // No trace collection should exist at all when neither parent nor sub-agent tracing is on.
            var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
            Assert.False(stats.Collections.ContainsKey(Constants.Documents.Collections.AiAgentConversationDebugCollection));
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_MultipleAttachments_PersistsAllNamesInOrder(Options options, GenAiConfiguration config)
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
            creationOptions: null, debug: true);

        chat.AddAttachment("heart.png", new MemoryStream(Convert.FromBase64String(AiTestData.HeartPngBase64)), "image/png");
        chat.AddAttachment("star.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png");
        chat.SetUserPrompt("Describe both images briefly.");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using var session = store.OpenAsyncSession();
        var tracesList = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.True(tracesList.Count == 1);

        var first = tracesList.First();
        Assert.NotNull(first.RequestBody);

        // AttachmentNames captures both files, in the order they were added.
        Assert.NotNull(first.AttachmentNames);
        Assert.Equal(new[] { "heart.png", "star.png" }, first.AttachmentNames);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_HiddenAttachment_NotSentToModel(Options options, GenAiConfiguration config)
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
            creationOptions: null, debug: true);
        chat.AddAttachment("heart.png", new MemoryStream(Convert.FromBase64String(AiTestData.HeartPngBase64)), "image/png");
        chat.AddAttachment("star.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png", new AttachmentOptions { SendToModel = false });
        chat.SetUserPrompt("What do you see in this image?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using (var session = store.OpenAsyncSession())
        {
            var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
            Assert.Single(traces);
            Assert.Equal(new[] { "heart.png" }, traces[0].AttachmentNames);
            Assert.DoesNotContain("star.png", traces[0].RequestBody);

            var doc = await session.LoadAsync<ConversationDocShape>(chat.Id);
            Assert.Equal(new[] { "star.png" }, doc.AttachmentsHiddenFromModel);
            Assert.DoesNotContain(doc.Messages, m => m.ToString().Contains("star.png"));

            var names = session.Advanced.Attachments.GetNames(doc).Select(a => a.Name).ToList();
            Assert.Contains("heart.png", names);
            Assert.Contains("star.png", names);
        }

        chat.SetUserPrompt("Which files do you have?");
        r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using (var session = store.OpenAsyncSession())
        {
            var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
            Assert.True(traces.Count >= 2);
            Assert.All(traces, t => Assert.DoesNotContain("star.png", t.RequestBody));
            Assert.Contains(traces, t => t.RequestBody.Contains("heart.png"));
        }

        Assert.Equal(new[] { "star.png" }, (await store.AI.GetConversationMessagesAsync(chat.Id)).AttachmentsHiddenFromModel);

        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.Attachments.Delete(chat.Id, "star.png");
            await session.SaveChangesAsync();
        }

        Assert.Empty((await store.AI.GetConversationMessagesAsync(chat.Id)).AttachmentsHiddenFromModel);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task HiddenAttachment_OnlyHiddenOnNewConversation_ThrowsHiddenOnlyError(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Describe images concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/", creationOptions: null);
        chat.AddAttachment("star.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png", new AttachmentOptions { SendToModel = false });

        var error = await Assert.ThrowsAnyAsync<Exception>(() => chat.RunAsync<OutputSchema>(CancellationToken.None));
        Assert.Contains("only attachments that are hidden from the model", error.ToString());
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_HiddenAttachment_ReaddedVisible_Unhides(Options options, GenAiConfiguration config)
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
            creationOptions: null, debug: true);
        chat.AddAttachment("star.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png", new AttachmentOptions { SendToModel = false });
        chat.SetUserPrompt("Say hello.");
        await chat.RunAsync<OutputSchema>(CancellationToken.None);

        int tracesBefore;
        using (var countSession = store.OpenAsyncSession())
            tracesBefore = (await countSession.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).Count();

        chat.AddAttachment("star.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png", new AttachmentOptions { SendToModel = false });
        var error = await Assert.ThrowsAnyAsync<Exception>(() => chat.RunAsync<OutputSchema>(CancellationToken.None));
        Assert.Contains("without open action calls or user prompt", error.ToString());

        using (var countSession = store.OpenAsyncSession())
            Assert.Equal(tracesBefore, (await countSession.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).Count());

        chat.AddAttachment("star.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png");
        chat.SetUserPrompt("What do you see in this image?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using var session = store.OpenAsyncSession();
        var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.Contains(traces, t => t.AttachmentNames?.Contains("star.png") == true);

        var doc = await session.LoadAsync<ConversationDocShape>(chat.Id);
        Assert.Empty(doc.AttachmentsHiddenFromModel);

        var messages = await store.AI.GetConversationMessagesAsync(chat.Id);
        Assert.Empty(messages.AttachmentsHiddenFromModel);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_HiddenCopiedAttachment_NotSentToModel(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new { Name = "images" }, "images/1");
            session.Advanced.Attachments.Store("images/1", "heart.png", new MemoryStream(Convert.FromBase64String(AiTestData.HeartPngBase64)), "image/png");
            session.Advanced.Attachments.Store("images/1", "star.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png");
            await session.SaveChangesAsync();
        }

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Describe images concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/",
            creationOptions: null, debug: true);
        chat.CopyAttachmentFrom("images/1", "heart.png");
        chat.CopyAttachmentFrom("images/1", "star.png", new AttachmentOptions { SendToModel = false });
        chat.SetUserPrompt("What do you see in this image?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using (var session = store.OpenAsyncSession())
        {
            var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
            Assert.Single(traces);
            Assert.Equal(new[] { "heart.png" }, traces[0].AttachmentNames);
            Assert.DoesNotContain("star.png", traces[0].RequestBody);
            Assert.DoesNotContain(AiTestData.StarPngBase64, traces[0].RequestBody);

            var doc = await session.LoadAsync<ConversationDocShape>(chat.Id);
            Assert.Equal(new[] { "star.png" }, doc.AttachmentsHiddenFromModel);
            Assert.DoesNotContain(doc.Messages, m => m.ToString().Contains("star.png"));

            var names = session.Advanced.Attachments.GetNames(doc).Select(a => a.Name).ToList();
            Assert.Contains("heart.png", names);
            Assert.Contains("star.png", names);
        }

        var messages = await store.AI.GetConversationMessagesAsync(chat.Id);
        Assert.Equal(new[] { "star.png" }, messages.AttachmentsHiddenFromModel);

        chat.SetUserPrompt("Which files do you have?");
        r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using (var session = store.OpenAsyncSession())
        {
            var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
            Assert.True(traces.Count >= 2);
            Assert.All(traces, t => Assert.DoesNotContain("star.png", t.RequestBody));
            Assert.Contains(traces, t => t.RequestBody.Contains("heart.png"));
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void HiddenAttachment_DuplicateNameInSameTurn_Throws()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("debug-test-agent", "chats/", creationOptions: null);
        chat.AddAttachment("a.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png", new AttachmentOptions { SendToModel = false });
        Assert.Throws<InvalidOperationException>(() =>
            chat.AddAttachment("a.png", new MemoryStream(Convert.FromBase64String(AiTestData.HeartPngBase64)), "image/png"));

        var other = store.AI.Conversation("debug-test-agent", "chats/", creationOptions: null);
        other.AddAttachment("b.png", new MemoryStream(Convert.FromBase64String(AiTestData.HeartPngBase64)), "image/png");
        Assert.Throws<InvalidOperationException>(() => other.CopyAttachmentFrom("images/1", "B.PNG", new AttachmentOptions { SendToModel = false }));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void AttachmentOptions_InvalidCombinations_Throw()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("debug-test-agent", "chats/", creationOptions: null);

        var hiddenAndPersisted = Assert.Throws<InvalidOperationException>(() =>
            chat.AddAttachment("a.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png",
                new AttachmentOptions { SendToModel = false, PersistToModelHistory = true }));
        Assert.Contains(nameof(AttachmentOptions.SendToModel), hiddenAndPersisted.Message);

        var onDemandAndPersisted = Assert.Throws<InvalidOperationException>(() =>
            chat.CopyAttachmentFrom("images/1", "b.png", new AttachmentOptions { SendImmediately = false, PersistToModelHistory = true }));
        Assert.Contains(nameof(AttachmentOptions.SendImmediately), onDemandAndPersisted.Message);

        chat.AddAttachment("a.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png", new AttachmentOptions { SendToModel = false });
        chat.CopyAttachmentFrom("images/1", "b.png", new AttachmentOptions { SendImmediately = false });
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void AttachmentOptions_ServerSideParsing_ValidatesAndIgnoresNameCase()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var parsed = AbstractAiAgentProcessor.ReadAttachmentOptions(context.ReadObject(new DynamicJsonValue
        {
            ["Star.PNG"] = new DynamicJsonValue { [nameof(AttachmentOptions.SendImmediately)] = false }
        }, "attachment-options"));

        Assert.True(parsed.TryGetValue("star.png", out var options));
        Assert.True(options.SendToModel);
        Assert.False(options.SendImmediately);
        Assert.False(options.PersistToModelHistory);

        var invalidCombination = Assert.Throws<InvalidOperationException>(() => AbstractAiAgentProcessor.ReadAttachmentOptions(context.ReadObject(new DynamicJsonValue
        {
            ["a.png"] = new DynamicJsonValue
            {
                [nameof(AttachmentOptions.SendToModel)] = false,
                [nameof(AttachmentOptions.PersistToModelHistory)] = true
            }
        }, "attachment-options")));
        Assert.Contains(nameof(AttachmentOptions.SendToModel), invalidCombination.Message);

        var notAnObject = Assert.Throws<InvalidOperationException>(() => AbstractAiAgentProcessor.ReadAttachmentOptions(context.ReadObject(new DynamicJsonValue
        {
            ["a.png"] = null
        }, "attachment-options")));
        Assert.Contains("must be an object", notAnObject.Message);

        var nullFlag = Assert.Throws<InvalidOperationException>(() => AbstractAiAgentProcessor.ReadAttachmentOptions(context.ReadObject(new DynamicJsonValue
        {
            ["a.png"] = new DynamicJsonValue { [nameof(AttachmentOptions.SendToModel)] = null }
        }, "attachment-options")));
        Assert.Contains("must be true or false", nullFlag.Message);

        var duplicateByCase = Assert.Throws<InvalidOperationException>(() => AbstractAiAgentProcessor.ReadAttachmentOptions(context.ReadObject(new DynamicJsonValue
        {
            ["a.png"] = new DynamicJsonValue(),
            ["A.PNG"] = new DynamicJsonValue()
        }, "attachment-options")));
        Assert.Contains("more than once", duplicateByCase.Message);

        var camelCase = Assert.Throws<InvalidOperationException>(() => AbstractAiAgentProcessor.ReadAttachmentOptions(context.ReadObject(new DynamicJsonValue
        {
            ["a.png"] = new DynamicJsonValue { ["sendToModel"] = false }
        }, "attachment-options")));
        Assert.Contains("Unknown attachment option 'sendToModel'", camelCase.Message);

        var misspelled = Assert.Throws<InvalidOperationException>(() => AbstractAiAgentProcessor.ReadAttachmentOptions(context.ReadObject(new DynamicJsonValue
        {
            ["a.png"] = new DynamicJsonValue { ["SendToModell"] = false }
        }, "attachment-options")));
        Assert.Contains("Unknown attachment option 'SendToModell'", misspelled.Message);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task PersistToModelHistory_AttachmentIsResentInTheSameWireFormat()
    {
        using var store = GetDocumentStore();
        await StoreImageDocument(store);

        var agentConfig = CreateAttachmentsTestAgent();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var payloads = new List<JObject>();
        string conversationId;

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = CreateCapturingHandler(database, payloads);
            handler.Initialize(agentConfig, "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "What do you see in this image?",
                AttachmentCommands = CreateCopyCommand(database, "images/1", "star.png"),
                AttachmentOptions = new Dictionary<string, AttachmentOptions> { ["star.png"] = new() { PersistToModelHistory = true } }
            }, changeVector: null);

            await handler.HandleRequestAsync(context, CancellationToken.None);
            conversationId = handler.ConversationDocId;
        }

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = CreateCapturingHandler(database, payloads);
            handler.Initialize(agentConfig, conversationId, new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "What color is it?"
            }, changeVector: null);

            await handler.HandleRequestAsync(context, CancellationToken.None);
        }

        Assert.Equal(2, payloads.Count);

        var firstMessages = (JArray)payloads[0]["messages"];
        var secondMessages = (JArray)payloads[1]["messages"];

        var attachmentIndex = IndexOfMessageContaining(firstMessages, AiTestData.StarPngBase64);
        Assert.True(attachmentIndex > 0, "The persisted attachment must be sent on the first request");
        Assert.Equal("What do you see in this image?", firstMessages[attachmentIndex - 1]["content"]?.ToString());

        for (var i = 0; i <= attachmentIndex; i++)
            Assert.Equal(firstMessages[i].ToString(Newtonsoft.Json.Formatting.None), secondMessages[i].ToString(Newtonsoft.Json.Formatting.None));

        Assert.Equal(1, CountOccurrences(payloads[0].ToString(Newtonsoft.Json.Formatting.None), AiTestData.StarPngBase64));
        Assert.Equal(1, CountOccurrences(payloads[1].ToString(Newtonsoft.Json.Formatting.None), AiTestData.StarPngBase64));
        Assert.All(secondMessages, m => Assert.Null(m[ConversationDocument.AttachmentsContentProperty]));

        var messages = await store.AI.GetConversationMessagesAsync(conversationId);
        Assert.All(messages.Messages, m => Assert.DoesNotContain(AiTestData.StarPngBase64, m.Content ?? string.Empty));
        Assert.DoesNotContain(messages.Messages, m => m.Role == AiMessageRole.User && m.Content == null && (m.Attachments?.Count ?? 0) == 0);
        Assert.Contains("star.png", messages.Attachments);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task SendImmediatelyFalse_OnlyNameIsSentAndContentIsRetrievedOnDemand()
    {
        using var store = GetDocumentStore();
        await StoreImageDocument(store);

        var agentConfig = CreateAttachmentsTestAgent();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var payloads = new List<JObject>();

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = new ExposableMockLlmHandler(Server.ServerStore, database, onRequest: payload =>
            {
                payloads.Add(payload);
                var response = payloads.Count == 1
                    ? MockLlm.CreateToolCallResponse(ChatCompletionClient.Constants.ToolNames.RetrieveAttachment, "{\"names\":[\"star.png\"]}")
                    : MockLlm.CreateAnswerResponse("\"a star\"");
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(response) };
            })
            {
                Authentication = null
            };

            handler.Initialize(agentConfig, "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "What do you see in the attached file?",
                AttachmentCommands = CreateCopyCommand(database, "images/1", "star.png"),
                AttachmentOptions = new Dictionary<string, AttachmentOptions> { ["star.png"] = new() { SendImmediately = false } }
            }, changeVector: null);

            await handler.HandleRequestAsync(context, CancellationToken.None);
        }

        Assert.Equal(2, payloads.Count);

        var first = payloads[0].ToString(Newtonsoft.Json.Formatting.None);
        Assert.DoesNotContain(AiTestData.StarPngBase64, first);
        var retrieveTool = payloads[0]["tools"]!.Single(t => t["function"]?["name"]?.ToString() == ChatCompletionClient.Constants.ToolNames.RetrieveAttachment);
        Assert.Contains("star.png", retrieveTool["function"]!["description"]!.ToString());

        Assert.Contains(AiTestData.StarPngBase64, payloads[1].ToString(Newtonsoft.Json.Formatting.None));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task SendImmediatelyFalse_OnlyOnDemandOnNewConversation_Throws()
    {
        using var store = GetDocumentStore();
        await StoreImageDocument(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = CreateCapturingHandler(database, []);
            handler.Initialize(CreateAttachmentsTestAgent(), "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                AttachmentCommands = CreateCopyCommand(database, "images/1", "star.png"),
                AttachmentOptions = new Dictionary<string, AttachmentOptions> { ["star.png"] = new() { SendImmediately = false } }
            }, changeVector: null);

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.HandleRequestAsync(context, CancellationToken.None));
            Assert.Contains("not sent immediately", error.Message);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task MixedAttachmentOptions_PersistedContentStaysIdenticalAcrossToolLoopIterations()
    {
        using var store = GetDocumentStore();
        await StoreImageDocument(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var payloads = new List<JObject>();
        var secretBase64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(SecretText));
        var noteBase64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(NoteText));

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = new ExposableMockLlmHandler(Server.ServerStore, database, onRequest: payload =>
            {
                payloads.Add(payload);
                var response = payloads.Count == 1
                    ? MockLlm.CreateToolCallResponse(ChatCompletionClient.Constants.ToolNames.RetrieveAttachment, "{\"names\":[\"heart.png\"]}")
                    : MockLlm.CreateAnswerResponse("\"done\"");
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(response) };
            })
            {
                Authentication = null
            };

            handler.Initialize(CreateAttachmentsTestAgent(), "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "Compare the files.",
                AttachmentCommands = CreateCopyCommand(database, "images/1", "star.png", "heart.png", "secret.txt", "note.txt"),
                AttachmentOptions = new Dictionary<string, AttachmentOptions>(StringComparer.OrdinalIgnoreCase)
                {
                    ["star.png"] = new() { PersistToModelHistory = true },
                    ["heart.png"] = new() { SendImmediately = false },
                    ["secret.txt"] = new() { SendToModel = false }
                }
            }, changeVector: null);

            await handler.HandleRequestAsync(context, CancellationToken.None);
        }

        Assert.Equal(2, payloads.Count);

        var first = payloads[0].ToString(Newtonsoft.Json.Formatting.None);
        var second = payloads[1].ToString(Newtonsoft.Json.Formatting.None);

        Assert.Equal(1, CountOccurrences(first, AiTestData.StarPngBase64));
        Assert.Equal(1, CountOccurrences(first, noteBase64));
        Assert.DoesNotContain(AiTestData.HeartPngBase64, first);
        Assert.DoesNotContain(secretBase64, first);

        var retrieveTool = payloads[0]["tools"]!.Single(t => t["function"]?["name"]?.ToString() == ChatCompletionClient.Constants.ToolNames.RetrieveAttachment);
        var toolDescription = retrieveTool["function"]!["description"]!.ToString();
        Assert.Contains("heart.png", toolDescription);
        Assert.DoesNotContain("secret.txt", toolDescription);

        var firstMessages = (JArray)payloads[0]["messages"];
        var secondMessages = (JArray)payloads[1]["messages"];
        var persistedIndex = IndexOfMessageContaining(firstMessages, AiTestData.StarPngBase64);
        Assert.Equal("Compare the files.", firstMessages[persistedIndex - 1]["content"]?.ToString());
        for (var i = 0; i <= persistedIndex; i++)
            Assert.Equal(firstMessages[i].ToString(Newtonsoft.Json.Formatting.None), secondMessages[i].ToString(Newtonsoft.Json.Formatting.None));

        Assert.Equal(1, CountOccurrences(second, AiTestData.StarPngBase64));
        Assert.Equal(1, CountOccurrences(second, AiTestData.HeartPngBase64));
        Assert.DoesNotContain(secretBase64, second);
        Assert.DoesNotContain(SecretText, second);
        Assert.Equal(1, CountOccurrences(second, noteBase64));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task PersistToModelHistory_CopiedUnderDifferentName_UsesStoredNameOptions()
    {
        using var store = GetDocumentStore();
        await StoreImageDocument(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var payloads = new List<JObject>();
        string conversationId;

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var copy = CreateCopyCommand(database, "images/1", "star.png");
            copy.ParsedCommands.Array![0].DestinationName = "renamed.png";

            var handler = CreateCapturingHandler(database, payloads);
            handler.Initialize(CreateAttachmentsTestAgent(), "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "What do you see in this image?",
                AttachmentCommands = copy,
                AttachmentOptions = new Dictionary<string, AttachmentOptions>(StringComparer.OrdinalIgnoreCase) { ["renamed.png"] = new() { PersistToModelHistory = true } }
            }, changeVector: null);

            await handler.HandleRequestAsync(context, CancellationToken.None);
            conversationId = handler.ConversationDocId;
        }

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = CreateCapturingHandler(database, payloads);
            handler.Initialize(CreateAttachmentsTestAgent(), conversationId, new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "What color is it?"
            }, changeVector: null);

            await handler.HandleRequestAsync(context, CancellationToken.None);
        }

        Assert.Equal(2, payloads.Count);
        Assert.Equal(1, CountOccurrences(payloads[1].ToString(Newtonsoft.Json.Formatting.None), AiTestData.StarPngBase64));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_AttachmentOptions_ThroughClient_PersistsAndDefersContent(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName,
            "You are a helpful assistant. Describe images concisely.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/", creationOptions: null, debug: true);
        chat.AddAttachment("star.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png",
            new AttachmentOptions { PersistToModelHistory = true });
        chat.AddAttachment("heart.png", new MemoryStream(Convert.FromBase64String(AiTestData.HeartPngBase64)), "image/png",
            new AttachmentOptions { SendImmediately = false });
        chat.SetUserPrompt("What do you see in the star image?");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        int firstTurnTraces;
        using (var session = store.OpenAsyncSession())
        {
            var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
            firstTurnTraces = traces.Count;
            Assert.All(traces, t => Assert.Contains(AiTestData.StarPngBase64, t.RequestBody));
            Assert.All(traces, t => Assert.Contains("heart.png", t.RequestBody));
            Assert.Contains(traces, t => t.RequestBody.Contains(AiTestData.HeartPngBase64) == false);
        }

        chat.SetUserPrompt("What color was the star?");
        r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using (var session = store.OpenAsyncSession())
        {
            var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
            Assert.True(traces.Count > firstTurnTraces);
            Assert.All(traces, t => Assert.Contains(AiTestData.StarPngBase64, t.RequestBody));
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task SendImmediatelyFalse_RetrievedInTheSameCall_SkipsChatReduction()
    {
        using var store = GetDocumentStore();
        await StoreImageDocument(store);

        var agentConfig = CreateAttachmentsTestAgent();
        agentConfig.ChatTrimming = new AiAgentChatTrimmingConfiguration
        {
            Truncate = new AiAgentTruncateChat
            {
                MessagesLengthBeforeTruncate = 2,
                MessagesLengthAfterTruncate = 1
            }
        };

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var payloads = new List<JObject>();

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = new ExposableMockLlmHandler(Server.ServerStore, database, onRequest: payload =>
            {
                payloads.Add(payload);
                var response = payloads.Count == 1
                    ? MockLlm.CreateToolCallResponse(ChatCompletionClient.Constants.ToolNames.RetrieveAttachment, "{\"names\":[\"heart.png\"]}")
                    : MockLlm.CreateAnswerResponse("\"a heart\"");
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(response) };
            })
            {
                Authentication = null
            };

            handler.Initialize(agentConfig, "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "Describe the attached file.",
                AttachmentCommands = CreateCopyCommand(database, "images/1", "heart.png"),
                AttachmentOptions = new Dictionary<string, AttachmentOptions>(StringComparer.OrdinalIgnoreCase) { ["heart.png"] = new() { SendImmediately = false } }
            }, changeVector: null);

            await handler.HandleRequestAsync(context, CancellationToken.None);
        }

        Assert.Equal(2, payloads.Count);
        Assert.Contains("Describe the attached file.", payloads[1].ToString(Newtonsoft.Json.Formatting.None));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task AttachmentOptions_ForUnknownAttachment_Throws()
    {
        using var store = GetDocumentStore();
        await StoreImageDocument(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var copy = CreateCopyCommand(database, "images/1", "secret.txt");
            copy.ParsedCommands.Array![0].DestinationName = "renamed.txt";

            var handler = CreateCapturingHandler(database, []);
            handler.Initialize(CreateAttachmentsTestAgent(), "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "Hello",
                AttachmentCommands = copy,
                AttachmentOptions = new Dictionary<string, AttachmentOptions>(StringComparer.OrdinalIgnoreCase) { ["secret.txt"] = new() { SendToModel = false } }
            }, changeVector: null);

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.HandleRequestAsync(context, CancellationToken.None));
            Assert.Contains("'secret.txt'", error.Message);
            Assert.Contains("no attachment with that name", error.Message);
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Debug_AttachmentOptions_ChangedAfterAdd_DoNotAffectAddedAttachment(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("debug-test-agent", config.ConnectionStringName, "You are a helpful assistant.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };
        var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);

        var chat = store.AI.Conversation(createResult.Identifier, "chats/", creationOptions: null, debug: true);
        var attachmentOptions = new AttachmentOptions { SendToModel = false };
        chat.AddAttachment("secret.txt", new MemoryStream(System.Text.Encoding.UTF8.GetBytes(SecretText)), "text/plain", attachmentOptions);
        attachmentOptions.SendToModel = true;
        chat.AddAttachment("note.txt", new MemoryStream(System.Text.Encoding.UTF8.GetBytes(NoteText)), "text/plain", attachmentOptions);
        chat.SetUserPrompt("Summarize the note.");
        var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);

        using var session = store.OpenAsyncSession();
        var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{chat.Id}/{AiDebugTrace.TraceSegment}/")).ToList();
        Assert.NotEmpty(traces);
        Assert.All(traces, t => Assert.DoesNotContain(SecretText, t.RequestBody));
        Assert.Contains(traces, t => t.RequestBody.Contains(NoteText));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CopiedAttachment_StoredUnderDifferentName_IsShownByStoredName()
    {
        using var store = GetDocumentStore();
        await StoreImageDocument(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var payloads = new List<JObject>();
        string conversationId;

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var copy = CreateCopyCommand(database, "images/1", "star.png");
            copy.ParsedCommands.Array![0].DestinationName = "star(1).png";

            var handler = CreateCapturingHandler(database, payloads);
            handler.Initialize(CreateAttachmentsTestAgent(), "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "What do you see in this image?",
                AttachmentCommands = copy
            }, changeVector: null, debugOverride: true);

            await handler.HandleRequestAsync(context, CancellationToken.None);
            conversationId = handler.ConversationDocId;
        }

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = CreateCapturingHandler(database, payloads);
            handler.Initialize(CreateAttachmentsTestAgent(), conversationId, new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "What color is it?"
            }, changeVector: null);

            await handler.HandleRequestAsync(context, CancellationToken.None);
        }

        using (var session = store.OpenAsyncSession())
        {
            var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{conversationId}/{AiDebugTrace.TraceSegment}/")).ToList();
            var attachmentTrace = Assert.Single(traces, t => t.AttachmentNames != null);
            Assert.Equal(new[] { "star(1).png" }, attachmentTrace.AttachmentNames);
        }

        Assert.Equal(2, payloads.Count);
        var secondTurn = payloads[1].ToString(Newtonsoft.Json.Formatting.None);
        Assert.Contains("[Attachments: star(1).png]", secondTurn);
        Assert.DoesNotContain("[Attachments: star.png]", secondTurn);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CopiedAttachment_StoredUnderDifferentName_OnDemandRetrieveUsesStoredName()
    {
        using var store = GetDocumentStore();
        await StoreImageDocument(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var payloads = new List<JObject>();

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var copy = CreateCopyCommand(database, "images/1", "star.png");
            copy.ParsedCommands.Array![0].DestinationName = "star(1).png";

            var handler = new ExposableMockLlmHandler(Server.ServerStore, database, onRequest: payload =>
            {
                payloads.Add(payload);
                var response = payloads.Count == 1
                    ? MockLlm.CreateToolCallResponse(ChatCompletionClient.Constants.ToolNames.RetrieveAttachment, "{\"names\":[\"star(1).png\"]}")
                    : MockLlm.CreateAnswerResponse("\"a star\"");
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(response) };
            })
            {
                Authentication = null
            };

            handler.Initialize(CreateAttachmentsTestAgent(), "chats/", new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = "What do you see in the attached file?",
                AttachmentCommands = copy,
                AttachmentOptions = new Dictionary<string, AttachmentOptions>(StringComparer.OrdinalIgnoreCase) { ["star(1).png"] = new() { SendImmediately = false } }
            }, changeVector: null);

            await handler.HandleRequestAsync(context, CancellationToken.None);
        }

        Assert.Equal(2, payloads.Count);
        var toolMessage = ((JArray)payloads[1]["messages"]).Single(m => m["role"]?.ToString() == "tool");
        Assert.Equal("Attachment: star(1).png", toolMessage["content"]?.ToString());
        Assert.Contains(AiTestData.StarPngBase64, payloads[1].ToString(Newtonsoft.Json.Formatting.None));
    }

    private static AiAgentConfiguration CreateAttachmentsTestAgent() =>
        new("attachments-test-agent", "fake-connection", "You are a helpful assistant.")
        {
            SampleObject = "{\"Answer\":\"answer here\"}"
        };

    private static async Task StoreImageDocument(Raven.Client.Documents.IDocumentStore store)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new { Name = "images" }, "images/1");
        session.Advanced.Attachments.Store("images/1", "star.png", new MemoryStream(Convert.FromBase64String(AiTestData.StarPngBase64)), "image/png");
        session.Advanced.Attachments.Store("images/1", "heart.png", new MemoryStream(Convert.FromBase64String(AiTestData.HeartPngBase64)), "image/png");
        session.Advanced.Attachments.Store("images/1", "secret.txt", new MemoryStream(System.Text.Encoding.UTF8.GetBytes(SecretText)), "text/plain");
        session.Advanced.Attachments.Store("images/1", "note.txt", new MemoryStream(System.Text.Encoding.UTF8.GetBytes(NoteText)), "text/plain");
        await session.SaveChangesAsync();
    }

    private ExposableMockLlmHandler CreateCapturingHandler(DocumentDatabase database, List<JObject> payloads) =>
        new(Server.ServerStore, database, onRequest: payload =>
        {
            payloads.Add(payload);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(MockLlm.CreateAnswerResponse("\"ok\"")) };
        })
        {
            Authentication = null
        };

    private const string SecretText = "top secret text that must never reach the model";
    private const string NoteText = "a visible note that is sent only in this call";

    private static MergedBatchCommand CreateCopyCommand(DocumentDatabase database, string sourceDocumentId, params string[] names) =>
        new(database)
        {
            ParsedCommands = new ArraySegment<BatchRequestParser.CommandData>(names.Select(name => new BatchRequestParser.CommandData
            {
                Type = CommandType.AttachmentCOPY,
                Id = sourceDocumentId,
                Name = name,
                DestinationId = "__this__",
                DestinationName = name
            }).ToArray())
        };

    private static int IndexOfMessageContaining(JArray messages, string value)
    {
        for (var i = 0; i < messages.Count; i++)
        {
            if (messages[i].ToString(Newtonsoft.Json.Formatting.None).Contains(value))
                return i;
        }

        return -1;
    }

    private static int CountOccurrences(string text, string value)
    {
        var count = 0;
        for (var index = text.IndexOf(value, StringComparison.Ordinal); index >= 0; index = text.IndexOf(value, index + value.Length, StringComparison.Ordinal))
            count++;

        return count;
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

    // Captured by CreateStoreWithSubAgentArithmetic so each test method can look up the parent agent
    // identifier without repeating the agent-creation boilerplate.
    private string _parentAgentIdentifier;

    private async Task<Raven.Client.Documents.IDocumentStore> CreateStoreWithSubAgentArithmetic(Options options, GenAiConfiguration config)
    {
        var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var subAgent = new AiAgentConfiguration("debug-sub-agent", config.ConnectionStringName,
            "You answer arithmetic questions. Return a short numeric answer.")
        {
            SampleObject = "{\"Answer\":\"42\"}"
        };
        var subAgentResult = await store.AI.CreateAgentAsync(subAgent, OutputSchema.Instance);

        var parentAgent = new AiAgentConfiguration("debug-parent-agent", config.ConnectionStringName,
            "You MUST delegate every arithmetic question to the math sub-agent. Never answer arithmetic directly. Return the sub-agent's answer.")
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
        _parentAgentIdentifier = parentResult.Identifier;
        return store;
    }

    private static async Task<DebugTraceSnapshot> LoadDebugSnapshot(Raven.Client.Documents.IDocumentStore store, string parentId)
    {
        using var session = store.OpenAsyncSession();
        var parentDoc = await session.LoadAsync<ConversationDocShape>(parentId);
        Assert.NotNull(parentDoc);

        var subId = parentDoc.SubConversationIds.FirstOrDefault();
        ConversationDocShape subDoc = null;
        if (subId != null)
            subDoc = await session.LoadAsync<ConversationDocShape>(subId);

        var parentTraces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{parentId}/{AiDebugTrace.TraceSegment}/")).Count();
        var subTraces = subId == null
            ? 0
            : (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>($"{subId}/{AiDebugTrace.TraceSegment}/")).Count();

        return new DebugTraceSnapshot
        {
            ParentTraces = parentTraces,
            SubAgentTraces = subTraces,
            ParentPersisted = parentDoc.Debug,
            SubPersisted = subDoc?.Debug ?? false,
            SubId = subId
        };
    }

    private sealed class DebugTraceSnapshot
    {
        public int ParentTraces { get; init; }
        public int SubAgentTraces { get; init; }
        public bool ParentPersisted { get; init; }
        public bool SubPersisted { get; init; }
        public string SubId { get; init; }
    }

    private class DebugTraceDoc
    {
        // Raw request body as written to the wire. Always a string — no parse is attempted server-side.
        public string RequestBody { get; set; }
        public List<string> AttachmentNames { get; set; }
        public object Response { get; set; }
        public object StreamEvents { get; set; }
    }

    private class ConversationDocShape
    {
        public bool Debug { get; set; }
        public HashSet<string> SubConversationIds { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        public List<string> AttachmentsHiddenFromModel { get; set; } = [];
        public List<JObject> Messages { get; set; } = [];
    }
}
