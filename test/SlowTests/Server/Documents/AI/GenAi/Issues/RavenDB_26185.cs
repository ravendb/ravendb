using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using SlowTests.Server.Documents.AI.AiAgent;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;
public class RavenDB_26185(ITestOutputHelper output) : RavenTestBase(output)
{
    private readonly ITestOutputHelper _output = output;

    private const string SimpleSchema =
        @"{""name"":""r"",""strict"":true,""schema"":{""type"":""object"",""properties"":{""Answer"":{""type"":""string""}},""required"":[""Answer""],""additionalProperties"":false}}";
    private const string MalformedJson = "}";
    private const string TruncatedJsonEndOfStream = "{\"Answer\":\"incomplete";
    private const string ValidJson = "{\"Answer\":\"complete\"}";


    [RavenFact(RavenTestCategory.Ai)]
    public async Task ValidJson_FinishReasonLength_ThrowsAiLength()
    {
        using var pool = CreateParserPool();
        using var client = CreateParserClient(pool, BuildResponse(ValidJson, finishReason: "length"));

        var ex = await Assert.ThrowsAsync<AiLengthException>(RunAsync(client));

        Assert.Equal("length", ex.FinishReason);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task EndOfStreamShape_FinishReasonLength_ThrowsAiLengthBeforeParsing()
    {
        using var pool = CreateParserPool();
        using var client = CreateParserClient(pool, BuildResponse(TruncatedJsonEndOfStream, finishReason: "length"));

        var ex = await Assert.ThrowsAsync<AiLengthException>(RunAsync(client));

        Assert.Equal("length", ex.FinishReason);
        Assert.Null(ex.InnerException); // proves parsing did not occur
    }

    // Malformed content + finish_reason "stop" -> the raw parser failure propagates (retryable path). It must NOT be
    // classified as a deterministic failure (AiLengthException / RefusedToAnswerException), which would stop retries.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task FinishReasonStop_MalformedContent_StaysRetryable()
    {
        using var pool = CreateParserPool();
        using var client = CreateParserClient(pool, BuildResponse(MalformedJson, finishReason: "stop"));

        var ex = await Record.ExceptionAsync(RunAsync(client));

        Assert.NotNull(ex); // malformed content must fail to parse
        Assert.False(ex is AiLengthException or RefusedToAnswerException,
            $"non-length malformed content must stay on the retryable path, not be a deterministic failure; got {ex.GetType().Name}");
    }

    // Same with no finish_reason at all -> still non-length -> raw parser failure, not a deterministic classification.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task NoFinishReason_MalformedContent_StaysRetryable()
    {
        using var pool = CreateParserPool();
        using var client = CreateParserClient(pool, BuildResponse(MalformedJson, finishReason: null));

        var ex = await Record.ExceptionAsync(RunAsync(client));

        Assert.NotNull(ex);
        Assert.False(ex is AiLengthException or RefusedToAnswerException,
            $"non-length malformed content must stay on the retryable path, not be a deterministic failure; got {ex.GetType().Name}");
    }

    // No usable content + a refusal (non-length) -> RefusedToAnswerException, raised from GetContent's no-content
    // branch (the original behavior). Refusal is NOT evaluated on responses that already carry content.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task MissingContent_WithRefusal_ThrowsRefusedToAnswer()
    {
        using var pool = CreateParserPool();
        using var client = CreateParserClient(pool, BuildResponse(content: null, finishReason: "stop", refusal: "I can't help with that"));

        var ex = await Assert.ThrowsAsync<RefusedToAnswerException>(RunAsync(client));

        Assert.Contains("I can't help with that", ex.Message);
    }

    // missing content + no refusal + non-length finish_reason -> existing "No response content"
    // UnexpectedResponseException (retryable), unchanged.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task MissingContent_NoRefusal_NonLength_ThrowsUnexpectedResponse()
    {
        using var pool = CreateParserPool();
        using var client = CreateParserClient(pool, BuildResponse(content: null, finishReason: "stop"));

        var ex = await Assert.ThrowsAsync<UnexpectedResponseException>(RunAsync(client));

        Assert.Contains("No response content", ex.Message);
    }

    // Ordering decision: missing content + no refusal + finish_reason "length" -> AiLengthException. The length signal
    // is deterministic even when the model produced no content at all (it hit the token limit before emitting a usable
    // answer); only a refusal outranks it.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task MissingContent_NoRefusal_FinishReasonLength_ThrowsAiLength()
    {
        using var pool = CreateParserPool();
        using var client = CreateParserClient(pool, BuildResponse(content: null, finishReason: "length"));

        var ex = await Assert.ThrowsAsync<AiLengthException>(RunAsync(client));

        Assert.Equal("length", ex.FinishReason);
    }

    // ---- Tool-call priority (global order: length -> tool calls) ----

    // Length + tool calls -> AiLengthException (NOT AiResponseType.Tool). The tool call may be incomplete
    // because generation stopped at the token limit, so it must not be returned/parsed/executed.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task ToolCalls_Length_NoRefusal_ThrowsAiLength_NotTool()
    {
        using var pool = CreateParserPool();
        using var client = CreateParserClient(pool, BuildToolCallResponse(finishReason: "length", refusal: null));

        var ex = await Assert.ThrowsAsync<AiLengthException>(RunAsync(client));

        Assert.Equal("length", ex.FinishReason);
    }

    // Normal tool-call response (finish_reason "tool_calls") -> AiResponseType.Tool with name + arguments preserved.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task ToolCalls_FinishReasonToolCalls_ReturnsToolResponse()
    {
        using var pool = CreateParserPool();
        using var client = CreateParserClient(pool,
            BuildToolCallResponse(finishReason: "tool_calls", refusal: null, toolName: "products-by-category", args: "{\"category\":\"grains\"}"));

        using (pool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            var msg = ctx.ReadObject(new DynamicJsonValue { ["role"] = "user", ["content"] = "u" }, "u");
            using var request = client.CreateCompletionRequest(ctx, [msg], attachments: null, tools: null, useTools: false, streaming: false, SimpleSchema);
            var r = await client.CompleteAsync(ctx, request, new AiUsage(), SimpleSchema, trace: null, token: default);

            Assert.Equal(AiResponseType.Tool, r.Type);
            var call = Assert.Single(r.ToolCalls);
            Assert.Equal("products-by-category", call.Name);
            Assert.Equal("{\"category\":\"grains\"}", call.Arguments);
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.AzureOpenAI | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task TokenLimitTruncation_IsReportedAsLength_AcrossProviders(Options options, GenAiConfiguration config)
    {
        using var pool = CreateParserPool();
        using var client = ChatCompletionClient.CreateChatCompletionClient(pool, config.Connection);

        var conn = config.Connection;
        var provider = conn.GetActiveProvider();
        // The connection already carries the model for its active provider (only one settings object is set) — read it
        // rather than pinning strings that get deprecated.
        var model = conn.OpenAiSettings?.Model ?? conn.AzureOpenAiSettings?.Model ?? conn.GoogleSettings?.Model;

        using (pool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            var schema = ctx.Sync.ReadForMemory(SimpleSchema, "truncation-probe/schema");

            // Mirror a real chat request (model + messages + response_format) but cap the output at a single token,
            // so the model is forced to stop at the limit and report the truncation via finish_reason.
            client.ForTestingPurposesOnly().ModifyPayload = writer =>
            {
                writer.WriteStartObject();
                writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.Model);
                writer.WriteString(model);
                writer.WriteComma();
                writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.Messages);
                writer.WriteStartArray();
                writer.WriteStartObject();
                writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.Role);
                writer.WriteString(ChatCompletionClient.Constants.RequestFields.RoleUserValue);
                writer.WriteComma();
                writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.Content);
                writer.WriteString("Write an extremely long, detailed, multi-section essay about the full history of computing.");
                writer.WriteEndObject();
                writer.WriteEndArray();
                writer.WriteComma();
                writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.ResponseFormat);
                writer.WriteStartObject();
                writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.Type);
                writer.WriteString(ChatCompletionClient.Constants.RequestFields.JsonSchema);
                writer.WriteComma();
                writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.JsonSchema);
                writer.WriteObject(schema);
                writer.WriteEndObject();
                writer.WriteComma();
                writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.MaxCompletionToken);
                writer.WriteInteger(50);
                writer.WriteEndObject();
            };

            var userMsg = ctx.ReadObject(new DynamicJsonValue { ["role"] = "user", ["content"] = "x" }, "truncation-probe/user");
            using var request = client.CreateCompletionRequest(ctx, [userMsg], attachments: null, tools: null,
                useTools: false, streaming: false, schema: SimpleSchema);

            var trace = new AiDebugTrace();
            Exception thrown = null;
            try
            {
                // Expected: finish_reason == "length" -> AiLengthException, thrown before any content parsing.
                await client.CompleteAsync(ctx, request, new AiUsage(), SimpleSchema, trace, default);
            }
            catch (Exception e)
            {
                thrown = e;
            }

            // trace.Response is captured before any throw, so the real finish_reason is available regardless of outcome.
            string finishReason = null;
            if (trace.Response != null
                && trace.Response.TryGet(ChatCompletionClient.Constants.ResponseFields.Choices, out BlittableJsonReaderArray choices)
                && choices.Length > 0
                && choices[0] is BlittableJsonReaderObject choice0)
                choice0.TryGet(ChatCompletionClient.Constants.ResponseFields.FinishReason, out finishReason);

            // Discovery log: records the real finish_reason for every provider run (visible in the test output).
            _output.WriteLine($"[RavenDB-26185] provider={provider} model='{model}' finish_reason='{finishReason ?? "<null>"}' " +
                              $"outcome={thrown?.GetType().Name ?? "Result"} message={thrown?.Message}");

            Assert.True(thrown is AiLengthException,
                $"Provider '{provider}' (model '{model}'): expected AiLengthException on token-limit truncation, but got " +
                $"'{thrown?.GetType().Name ?? "no exception (a result was returned)"}'. Actual finish_reason = '{finishReason ?? "<null>"}'. " +
                $"Exception message: {thrown?.Message}. " +
                $"A non-'length' value (e.g. 'MAX_TOKENS') means the shared finish_reason == \"length\" check does not cover " +
                $"this provider and needs a provider-specific mapping.");

            Assert.Equal("length", ((AiLengthException)thrown).FinishReason, ignoreCase: true);
        }
    }

    private static Func<Task> RunAsync(ChatCompletionClient client)
        => () => client.TestCompleteAsync("system", "user", SimpleSchema, default);

    private static TransactionContextPool CreateParserPool()
        => new(RavenLogManager.Instance.CreateNullLogger(), new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()));

    private static MockLlm CreateParserClient(IMemoryContextPool contextPool, HttpResponseMessage response)
    {
        var settings = new OpenAiChatCompletionClientSettings(new OpenAiSettings("fake-key", "https://fake.invalid/", "gpt-4o"));

        // The response is consumed once by CompleteAsync, which is all these single-shot tests need.
        return new MockLlm(contextPool, settings, onRequest: _ => response, conventions: ChatCompletionClient.ConventionsToUse);
    }

    private static HttpResponseMessage BuildResponse(string content, string finishReason, string refusal = null)
    {
        // 'content' is the model's output carried as a JSON *string* inside a valid envelope. Encoding it with
        // JsonConvert.ToString keeps the envelope valid even when the string itself decodes to invalid JSON.
        string contentField = content == null ? "null" : JsonConvert.ToString(content);
        string refusalField = refusal == null ? "null" : JsonConvert.ToString(refusal);
        string finishReasonField = finishReason == null ? "null" : JsonConvert.ToString(finishReason);

        var json = $$"""
            {
                "id": "chatcmpl-26185",
                "object": "chat.completion",
                "model": "gpt-4o",
                "choices": [{
                    "index": 0,
                    "message": { "role": "assistant", "content": {{contentField}}, "refusal": {{refusalField}}, "annotations": [] },
                    "logprobs": null,
                    "finish_reason": {{finishReasonField}}
                }],
                "usage": { "prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10 }
            }
            """;

        return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json) };
    }

    // Builds a tool-call response (message.content == null, one tool_call). 'arguments' is carried as a JSON string, as
    // the provider does. finishReason/refusal are configurable so we can exercise the global priority order.
    private static HttpResponseMessage BuildToolCallResponse(string finishReason, string refusal, string toolName = "my-tool", string args = "{}")
    {
        string refusalField = refusal == null ? "null" : JsonConvert.ToString(refusal);
        string finishReasonField = finishReason == null ? "null" : JsonConvert.ToString(finishReason);

        var json = $$"""
            {
                "id": "chatcmpl-26185",
                "object": "chat.completion",
                "model": "gpt-4o",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [{ "id": "call_1", "type": "function", "function": { "name": {{JsonConvert.ToString(toolName)}}, "arguments": {{JsonConvert.ToString(args)}} } }],
                        "refusal": {{refusalField}},
                        "annotations": []
                    },
                    "logprobs": null,
                    "finish_reason": {{finishReasonField}}
                }],
                "usage": { "prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10 }
            }
            """;

        return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json) };
    }

    private const string StreamProperty = "Answer";

    // content chunks delivered to the callback, then a terminal delta-less event with finish_reason "length"
    // -> AiLengthException after the callback already saw partial output; no successful AiResponse.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task Streaming_Content_Then_Length_ThrowsAiLength_AfterCallbackSawContent()
    {
        using var pool = CreateParserPool();
        var callback = new StringBuilder();
        var sse = Sse(ContentChunk("{\"Answer\":\""), ContentChunk("hello world"), ContentChunk("\"}"), FinishChunk("length"));

        var ex = await Assert.ThrowsAsync<AiLengthException>(() => RunStreamingAsync(pool, sse, callback));

        Assert.Equal("length", ex.FinishReason);
        Assert.Contains("hello world", callback.ToString()); // partial content was streamed before the terminal signal
    }

    // tool-call chunks then finish_reason "length" -> AiLengthException; AiResponseType.Tool is never returned.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task Streaming_ToolCall_Then_Length_ThrowsAiLength_NotTool()
    {
        using var pool = CreateParserPool();
        var callback = new StringBuilder();
        var sse = Sse(ToolCallChunk("products-by-category", "{\"category\":\"grains\"}"), FinishChunk("length"));

        var ex = await Assert.ThrowsAsync<AiLengthException>(() => RunStreamingAsync(pool, sse, callback));

        Assert.Equal("length", ex.FinishReason);
    }

    // normal streamed tool call (finish_reason "tool_calls") -> AiResponseType.Tool, name + arguments preserved.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task Streaming_NormalToolCall_ReturnsToolResponse()
    {
        using var pool = CreateParserPool();
        var callback = new StringBuilder();
        var sse = Sse(ToolCallChunk("products-by-category", "{\"category\":\"grains\"}"), FinishChunk("tool_calls"));

        var r = await RunStreamingAsync(pool, sse, callback);

        Assert.Equal(AiResponseType.Tool, r.Type);
        var call = Assert.Single(r.ToolCalls);
        Assert.Equal("products-by-category", call.Name);
        Assert.Equal("{\"category\":\"grains\"}", call.Arguments);
    }

    // normal streamed result (finish_reason "stop") -> AiResponseType.Result; callback received the streamed data.
    [RavenFact(RavenTestCategory.Ai)]
    public async Task Streaming_NormalResult_ReturnsResult_AndCallbackSawContent()
    {
        using var pool = CreateParserPool();
        var callback = new StringBuilder();
        var sse = Sse(ContentChunk("{\"Answer\":\""), ContentChunk("hello world"), ContentChunk("\"}"), FinishChunk("stop"));

        var r = await RunStreamingAsync(pool, sse, callback);

        Assert.Equal(AiResponseType.Result, r.Type);
        Assert.Contains("hello world", callback.ToString());
    }

    private static async Task<AiResponse> RunStreamingAsync(TransactionContextPool pool, string sseBody, StringBuilder callbackSink)
    {
        var settings = new OpenAiChatCompletionClientSettings(new OpenAiSettings("fake-key", "https://fake.invalid/", "gpt-4o"));
        using var client = new StreamingMockLlm(pool, settings, sseBody);

        using (pool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            var msg = ctx.ReadObject(new DynamicJsonValue { ["role"] = "user", ["content"] = "u" }, "u");
            using var request = client.CreateCompletionRequest(ctx, [msg], attachments: null, tools: null, useTools: false, streaming: true, SimpleSchema);

            return await client.StreamingCompleteAsync(ctx, pool, StreamProperty, request,
                data =>
                {
                    callbackSink.Append(Encoding.UTF8.GetString(data.Span));
                    return Task.CompletedTask;
                },
                new AiUsage(), SimpleSchema, trace: null, token: default);
        }
    }

    private static string Sse(params string[] jsonEvents)
    {
        var sb = new StringBuilder();
        foreach (var e in jsonEvents)
            sb.Append("data: ").Append(e).Append("\n\n");
        sb.Append("data: [DONE]\n\n");
        return sb.ToString();
    }

    private static string ContentChunk(string contentPiece)
        => "{\"choices\":[{\"index\":0,\"delta\":{\"content\":" + JsonConvert.ToString(contentPiece) + "},\"finish_reason\":null}]}";

    private static string ToolCallChunk(string name, string args)
        => "{\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":"
           + JsonConvert.ToString(name) + ",\"arguments\":" + JsonConvert.ToString(args) + "}}]},\"finish_reason\":null}]}";

    // A terminal event that carries finish_reason with NO delta field - exercises tracking finish_reason OUTSIDE the
    // delta branch (some providers emit the terminal chunk without a delta).
    private static string FinishChunk(string finishReason)
        => "{\"choices\":[{\"index\":0,\"finish_reason\":" + JsonConvert.ToString(finishReason) + "}]}";

    // Test-only client that returns a handcrafted text/event-stream response instead of calling a provider.
    private sealed class StreamingMockLlm : ChatCompletionClient
    {
        private readonly string _sseBody;

        public StreamingMockLlm(IMemoryContextPool contextPool, AbstractChatCompletionClientSettings settings, string sseBody)
            : base(contextPool, settings, ConventionsToUse)
        {
            _sseBody = sseBody;
        }

        protected override Task<HttpResponseMessage> SendStreamingRequestAsync(HttpRequestMessage request, CancellationToken token)
        {
            var content = new StringContent(_sseBody);
            content.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content });
        }
    }

    private const string BadMarker = "ZZZ_BAD_26185";

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    public async Task PartialBatch_TruncatedContent_IsRecordedAndNotRetried()
    {
        using var store = GetDocumentStore();
        var db = await GetDatabase(store.Database);

        var badCalls = 0;
        var config = await SetupOfflineGenAiAsync(store, db, "partial-26185", payload =>
        {
            // The marker only appears in the failing document's request payload (via its context/user prompt).
            if (payload.ToString().Contains(BadMarker))
            {
                Interlocked.Increment(ref badCalls);
                // Truncated content + "length": the provider-signal validation throws AiLengthException before any
                // content parsing, so the truncation shape is irrelevant here.
                return BuildResponse("{\"Result\":\"incomplete", finishReason: "length");
            }

            return BuildResponse("{\"Result\":\"ok\"}", finishReason: "stop");
        });

        const string okDoc = "posts/1";
        const string failDoc = "posts/2";
        using (var session = store.OpenSession())
        {
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment("a normal comment", "author") { Id = "1" }], "t", "b"), okDoc);
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment($"{BadMarker} truncated", "author") { Id = "1" }], "t", "b"), failDoc);
            session.SaveChanges();
        }

        Assert.True(await WaitForValueAsync(() => HasHashAsync(store, okDoc, config.Identifier), true, timeout: 30_000),
            "the successful document should be hashed");
        Assert.True(await WaitForValueAsync(() => HasHashAsync(store, failDoc, config.Identifier), true, timeout: 30_000),
            "the truncated document should be recorded as a deterministic failure (hashed)");

        Assert.False(await HasRefreshAsync(store, okDoc), "a successful document must not be parked via @refresh");
        Assert.False(await HasRefreshAsync(store, failDoc), "a deterministic failure must not be parked via @refresh");

        Assert.True(await IsPatchedAsync(store, okDoc), "the successful document should have been patched by the update script");
        Assert.False(await IsPatchedAsync(store, failDoc), "the failed document must not be patched (no model output)");

        var callsAfterFirstCycle = Volatile.Read(ref badCalls);
        Assert.Equal(1, callsAfterFirstCycle);

        // Force another ETL cycle with an unrelated document and prove the unchanged failed document is not re-sent.
        const string thirdDoc = "posts/3";
        using (var session = store.OpenSession())
        {
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment("another normal comment", "author") { Id = "1" }], "t", "b"), thirdDoc);
            session.SaveChanges();
        }

        Assert.True(await WaitForValueAsync(() => HasHashAsync(store, thirdDoc, config.Identifier), true, timeout: 30_000),
            "the third document should be processed in a subsequent ETL cycle");
        Assert.Equal(1, Volatile.Read(ref badCalls)); // the unchanged failed document was never re-sent to the model
    }

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    public async Task PartialBatch_IncompleteJson_NonLength_IsRetryable_ParkedViaRefresh_AndResent()
    {
        using var store = GetDocumentStore();
        var db = await GetDatabase(store.Database);

        var badCalls = 0;
        var config = await SetupOfflineGenAiAsync(store, db, "partial-incomplete-26185", payload =>
        {
            if (payload.ToString().Contains(BadMarker))
            {
                Interlocked.Increment(ref badCalls);
                return BuildResponse(MalformedJson, finishReason: "stop"); // malformed JSON (deterministic parse failure), clean stop
            }

            return BuildResponse("{\"Result\":\"ok\"}", finishReason: "stop");
        });

        const string okDoc = "posts/1";
        const string failDoc = "posts/2";
        using (var session = store.OpenSession())
        {
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment("a normal comment", "author") { Id = "1" }], "t", "b"), okDoc);
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment($"{BadMarker} incomplete", "author") { Id = "1" }], "t", "b"), failDoc);
            session.SaveChanges();
        }

        Assert.True(await WaitForValueAsync(() => HasHashAsync(store, okDoc, config.Identifier), true, timeout: 30_000),
            "the successful document should be hashed");
        Assert.True(await IsPatchedAsync(store, okDoc), "the successful document should have been patched");

        Assert.True(await WaitForRefreshAsync(store, failDoc), "incomplete JSON without 'length' is retryable and must be parked via @refresh");
        Assert.False(await HasHashAsync(store, failDoc, config.Identifier), "a retryable failure must not be hashed");

        Assert.True(await WaitForValueAsync(() => Task.FromResult(Volatile.Read(ref badCalls) >= 2), true, timeout: 30_000),
            "the unchanged incomplete document is retryable and should be sent to the model again");
    }

    // Test C: a whole attempted batch where every response is incomplete JSON with a non-"length" finish_reason follows
    // the existing non-deterministic path - it throws (ETL fallback), stamping NO @refresh and NO hash - and is retried.
    // Once the model recovers (returns valid JSON) the document is processed, proving it was on the retryable path.
    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    public async Task WholeBatch_IncompleteJson_NonLength_IsNonDeterministic_RetriedViaFallback()
    {
        using var store = GetDocumentStore();
        var db = await GetDatabase(store.Database);

        var badCalls = 0;
        var failing = true;
        var config = await SetupOfflineGenAiAsync(store, db, "whole-incomplete-26185", payload =>
        {
            if (payload.ToString().Contains(BadMarker) == false)
                return BuildResponse("{\"Result\":\"ok\"}", finishReason: "stop");

            Interlocked.Increment(ref badCalls);
            return Volatile.Read(ref failing)
                ? BuildResponse(MalformedJson, finishReason: "stop") // malformed JSON (deterministic parse failure), non-"length"
                : BuildResponse("{\"Result\":\"ok\"}", finishReason: "stop");
        });

        const string failDoc = "posts/1";
        using (var session = store.OpenSession())
        {
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment($"{BadMarker} incomplete", "author") { Id = "1" }], "t", "b"), failDoc);
            session.SaveChanges();
        }

        Assert.True(await WaitForValueAsync(() => Task.FromResult(Volatile.Read(ref badCalls) >= 1), true, timeout: 30_000),
            "the document should be attempted");

        // A whole attempted non-deterministic batch throws before stamping @refresh, so it is neither parked nor hashed.
        Assert.False(await HasRefreshAsync(store, failDoc), "a whole-batch non-deterministic failure must not stamp @refresh");
        Assert.False(await HasHashAsync(store, failDoc, config.Identifier), "a non-deterministic failure must not be hashed");

        // Let the model recover: the document must eventually be processed, proving it stayed on the retryable path.
        Volatile.Write(ref failing, false);
        Assert.True(await WaitForValueAsync(() => HasHashAsync(store, failDoc, config.Identifier), true, timeout: 60_000),
            "once the model returns valid JSON the retried document should be processed");
    }

    private async Task<GenAiConfiguration> SetupOfflineGenAiAsync(IDocumentStore store, DocumentDatabase db, string identifier,
        Func<JObject, HttpResponseMessage> onRequest)
    {
        var connection = new AiConnectionString
        {
            Name = "fake-openai-" + identifier,
            Identifier = "fake-openai-" + identifier,
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings("fake-key", "https://fake.invalid/", "gpt-4o")
        };
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connection));

        var config = new GenAiConfiguration
        {
            Name = "genai-" + identifier,
            ConnectionStringName = connection.Name,
            Identifier = identifier,
            Collection = "Posts",
            Prompt = "Reply with the given text unchanged.",
            SampleObject = JsonConvert.SerializeObject(new { Result = "text" }),
            UpdateScript = @"const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].Result = $output.Result;",
            GenAiTransformation = new GenAiTransformation
            {
                Script = "for (const comment of this.Comments) ai.genContext({Text: comment.Text, Id: comment.Id});"
            },
            MaxConcurrency = 2 // keep both partial-batch documents in one batch
        };
        store.Maintenance.Send(new AddGenAiOperation(config));

        var etlProcess = await WaitForGenAiProcessAsync(db);

        var mock = new MockLlm(db.DocumentsStorage.ContextPool,
            new OpenAiChatCompletionClientSettings(new OpenAiSettings("fake-key", "https://fake.invalid/", "gpt-4o")),
            onRequest, conventions: ChatCompletionClient.ConventionsToUse);

        var field = typeof(GenAiTask).GetField("_chatCompletionClient", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(field);
        var previous = (ChatCompletionClient)field.GetValue(etlProcess);
        field.SetValue(etlProcess, mock);
        previous?.Dispose();

        return config;
    }

    private async Task<GenAiTask> WaitForGenAiProcessAsync(DocumentDatabase db)
    {
        GenAiTask etlProcess = null;
        Assert.True(await WaitForValueAsync(() =>
        {
            etlProcess = db.EtlLoader.Processes.OfType<GenAiTask>().FirstOrDefault();
            return Task.FromResult(etlProcess != null);
        }, true, timeout: 15_000), "GenAi ETL process was not loaded in time");
        return etlProcess;
    }

    private static async Task<bool> HasHashAsync(IDocumentStore store, string docId, string identifier)
    {
        using var session = store.OpenAsyncSession();
        var doc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
        return doc != null &&
               doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
               metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashes) &&
               hashes.TryGet(identifier, out BlittableJsonReaderArray arr) && arr.Length > 0;
    }

    private static async Task<bool> HasRefreshAsync(IDocumentStore store, string docId)
    {
        using var session = store.OpenAsyncSession();
        var doc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
        return doc != null &&
               doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
               metadata.TryGet(Constants.Documents.Metadata.Refresh, out object _);
    }

    private static Task<bool> WaitForRefreshAsync(IDocumentStore store, string docId)
        => WaitForValueAsync(() => HasRefreshAsync(store, docId), true, timeout: 30_000);

    private static async Task<bool> IsPatchedAsync(IDocumentStore store, string docId)
    {
        using var session = store.OpenAsyncSession();
        var doc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
        if (doc == null || doc.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments) == false || comments.Length == 0)
            return false;

        var comment = (BlittableJsonReaderObject)comments[0];
        return comment.TryGet("Result", out string _);
    }
}
