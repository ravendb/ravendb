using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

// The structured-output contract, streaming and non-streaming.
//
// A thinking model can answer with an empty 'content' and a large 'reasoning' block. Reasoning may contain
// chain-of-thought, so it is never parsed eagerly as structured content - doing so was the RavenDB-26944
// crash. For providers that place the answer in 'reasoning_content' / 'reasoning' (RavenDB-25681) it is a
// fallback used only when no 'content' arrived: unstructured output takes it as the answer text, structured
// output tries the complete buffered reasoning once as the structured answer. 'content', once it arrives, is
// always authoritative. A length-truncated response is incomplete and must fail with the length-specific
// error instead.
//
// Structured_LiveModel_StreamsOnlyTheAnswer_NotItsReasoning is the only test that uses an external provider.
// The remaining tests are deterministic: most inject provider responses through InjectingConversationHandler,
// while the public-client error propagation test uses a local fake SSE endpoint to exercise the complete
// client/server protocol.
public class RavenDB_26944(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string UserPrompt = "What is 2+2? Reply with just the number.";
    private const string SystemPrompt = "Answer the user's question. Respond immediately and briefly, without deliberation.";
    private const string AnswerProperty = "Answer";

    // an empty answer plus rambling reasoning - the reported Ollama failure shape
    private const string RamblingReasoning = "Okay, let's see. The user wants me to answer, but first let me think about it at length...";


    private class AnswerSchema
    {
        public string Answer = "a short answer";
    }

    // The injected tests never reach a provider, so any well-formed connection string will do.
    private const string FakeConnectionStringName = "ravendb-26944-connection";

    private static AiConnectionString FakeConnection() => new()
    {
        Name = FakeConnectionStringName,
        Identifier = FakeConnectionStringName,
        ModelType = AiModelType.Chat,
        OpenAiSettings = new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o")
    };

    #region streaming

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Structured_LiveModel_StreamsOnlyTheAnswer_NotItsReasoning(Options options, GenAiConfiguration config)
    {
        // Fully live through the public API. A local reasoning model emits its chain-of-thought before the
        // answer, the shape that used to crash the SSE parser. The streamed property must equal the parsed
        // answer: had any reasoning delta reached the answer parser, the two would differ. Nothing is
        // asserted about the model's wording.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("streaming-agent", config.ConnectionStringName, SystemPrompt);
        var identifier = (await store.AI.CreateAgentAsync(agent, new AnswerSchema())).Identifier;

        var chat = store.AI.Conversation(identifier, "chats/", new AiConversationCreationOptions());
        chat.SetUserPrompt(UserPrompt);

        var streamed = new StringBuilder();
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var result = await chat.StreamAsync<AnswerSchema>(x => x.Answer, chunk =>
        {
            streamed.Append(chunk);
            return Task.CompletedTask;
        }, cts.Token);

        Assert.Equal(AiConversationResult.Done, result.Status);
        Assert.Equal(result.Answer.Answer, streamed.ToString());
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("reasoning-only")]
    [InlineData("reasoning-content-only")]
    [InlineData("non-json-content")]
    [InlineData("incomplete-json-content")]
    [InlineData("json-array-content")]
    [InlineData("content-after-invalid")]
    [InlineData("json-reasoning-then-invalid-content")]
    public async Task Structured_UnusableStream_ThrowsCleanError_WithoutLeakingTheParserError(string shape)
    {
        var sse = shape switch
        {
            // prose-only reasoning - the RavenDB-25681 fallback must reject it cleanly, not crash the parser
            "reasoning-only" => Wire.Stream(Wire.ReasoningDelta(RamblingReasoning), Wire.FinishChunk("stop")),
            "reasoning-content-only" => Wire.Stream(Wire.ReasoningDelta(RamblingReasoning, reasoningContentField: true), Wire.FinishChunk("stop")),
            // the model streamed prose instead of JSON
            "non-json-content" => Wire.Stream(Wire.ContentDelta("Sure, here is the answer:"), Wire.FinishChunk("stop")),
            // the stream ended in the middle of the JSON object
            "incomplete-json-content" => Wire.Stream(Wire.ContentDelta("{\"Answer\":"), Wire.FinishChunk("stop")),
            // well-formed JSON, but an array root where the schema requires an object
            "json-array-content" => Wire.Stream(Wire.ContentDelta("[{\"Answer\":\"ok\"}]"), Wire.FinishChunk("stop")),
            // content keeps arriving after the parser rejected the stream - a later well-formed chunk must be
            // discarded, not resurrect the parse into a false success
            "content-after-invalid" => Wire.Stream(Wire.ContentDelta("{\"Answer\":"), Wire.ContentDelta("oops not json}"), Wire.ContentDelta("{\"Answer\":\"42\"}"), Wire.FinishChunk("stop")),
            // real content is authoritative: after invalid content the valid JSON reasoning must not be used
            "json-reasoning-then-invalid-content" => Wire.Stream(Wire.ReasoningDelta("{\"Answer\":\"first\"}"), Wire.ContentDelta("oops"), Wire.FinishChunk("stop")),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "unknown stream shape")
        };

        using var store = GetDocumentStore();
        var (_, error) = await StreamAsync(store, "chats/bad-stream-" + shape, InjectedResponse.Sse(sse));

        AssertNoParserErrorLeaked<UnexpectedResponseException>(error);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("valid-content")]
    [InlineData("invalid-content-before-length")]
    [InlineData("json-reasoning")]
    public async Task Structured_LengthTruncatedStream_AlwaysThrowsTooManyTokens(string shape)
    {
        var sse = shape switch
        {
            // a complete-looking answer that was still cut off: it must not be accepted
            "valid-content" => Wire.Stream(Wire.ContentDelta("{\"Answer\":\"42\"}"), Wire.FinishChunk("length")),
            // the parser rejected the content mid-stream; the later 'length' signal must still be observed
            "invalid-content-before-length" => Wire.Stream(Wire.ContentDelta("{\"Answer\":"), Wire.ContentDelta("oops not json}"), Wire.FinishChunk("length")),
            // a truncated response must not promote the reasoning fallback, even when it is valid JSON
            "json-reasoning" => Wire.Stream(Wire.ReasoningDelta("{\"Answer\":\"42\"}"), Wire.FinishChunk("length")),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "unknown stream shape")
        };

        using var store = GetDocumentStore();
        var (_, error) = await StreamAsync(store, "chats/length-stream-" + shape, InjectedResponse.Sse(sse));

        Assert.True(HasException<TooManyTokensException>(error), $"expected TooManyTokensException, got: {error}");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Structured_ValidPrefix_IsStreamedToClient_BeforeTheDeferredFailure()
    {
        // The incremental parser rejects the content mid-stream, but the valid prefix was already delivered
        // to the client and must stay delivered; the failure is reported afterwards.
        var sse = Wire.Stream(
            Wire.ContentDelta("{\"Answer\":\"valid prefix "),
            Wire.ContentDelta("text\" garbage"),
            Wire.FinishChunk("stop"));

        using var store = GetDocumentStore();
        var (streamed, error) = await StreamAsync(store, "chats/prefix-then-invalid", InjectedResponse.Sse(sse));

        Assert.True(HasException<UnexpectedResponseException>(error), $"expected UnexpectedResponseException, got: {error}");
        Assert.Contains("valid prefix ", streamed);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Unstructured_ReasoningOnlyStream_FallsBackToReasoning(bool reasoningContentField)
    {
        // RavenDB-25681: with no schema, a model that streams its answer only in 'reasoning' /
        // 'reasoning_content' is still usable and that text becomes the answer.
        const string answer = "the plain text answer";
        var sse = Wire.Stream(Wire.ReasoningDelta(answer, reasoningContentField), Wire.FinishChunk("stop"));

        using var store = GetDocumentStore();
        var (streamed, error) = await StreamAsync(store, $"chats/unstructured-stream-{reasoningContentField}",
            InjectedResponse.Sse(sse), noSchema: true);

        Assert.Null(error);
        Assert.Contains(answer, streamed);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("garbage-in-later-chunk")]
    [InlineData("garbage-in-same-chunk")]
    public async Task Structured_CompletedAnswer_IsKept_DespiteTrailingGarbage(string shape)
    {
        // Once the model completed a valid structured answer, that answer was already streamed to the client,
        // so trailing garbage is dropped rather than failing an answer that was delivered. The parse stops at
        // the first garbage either way: an answer completed before it wins, anything less is an error (see
        // 'content-after-invalid').
        var sse = shape switch
        {
            "garbage-in-later-chunk" => Wire.Stream(Wire.ContentDelta("{\"Answer\":\"42\"}"), Wire.ContentDelta("oops"), Wire.FinishChunk("stop")),
            "garbage-in-same-chunk" => Wire.Stream(Wire.ContentDelta("{\"Answer\":\"42\"}oops"), Wire.FinishChunk("stop")),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "unknown stream shape")
        };

        using var store = GetDocumentStore();
        var (streamed, error) = await StreamAsync(store, "chats/trailing-garbage-" + shape, InjectedResponse.Sse(sse));

        Assert.Null(error);
        Assert.Contains("42", streamed);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("json-in-reasoning")]
    [InlineData("json-in-reasoning-content")]
    [InlineData("json-split-across-chunks")]
    [InlineData("json-reasoning-then-content")]
    public async Task Structured_JsonReasoning_IsAcceptedAsFallback_OnlyWithoutContent(string shape)
    {
        // RavenDB-25681: some providers place the whole answer in 'reasoning' / 'reasoning_content'. The
        // complete buffered reasoning is tried once as the structured answer - but only when no 'content'
        // arrived; content, once seen, is always authoritative.
        var sse = shape switch
        {
            "json-in-reasoning" => Wire.Stream(Wire.ReasoningDelta("{\"Answer\":\"42\"}"), Wire.FinishChunk("stop")),
            "json-in-reasoning-content" => Wire.Stream(Wire.ReasoningDelta("{\"Answer\":\"42\"}", reasoningContentField: true), Wire.FinishChunk("stop")),
            "json-split-across-chunks" => Wire.Stream(Wire.ReasoningDelta("{\"Answer\":"), Wire.ReasoningDelta("\"42\"}"), Wire.FinishChunk("stop")),
            "json-reasoning-then-content" => Wire.Stream(Wire.ReasoningDelta("{\"Answer\":\"first\"}"), Wire.ContentDelta("{\"Answer\":\"42\"}"), Wire.FinishChunk("stop")),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "unknown stream shape")
        };

        using var store = GetDocumentStore();
        var (streamed, error) = await StreamAsync(store, "chats/reasoning-fallback-" + shape, InjectedResponse.Sse(sse));

        Assert.Null(error);
        // '42' must reach the streamed callback: for the reasoning fallback that is the
        // reasoningPromoted -> FlushStreamedPropertyAsync path
        Assert.Contains("42", streamed);
        Assert.DoesNotContain("first", streamed);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ToolCalls_SplitByReasoningDeltas_AreNotMergedIntoOne()
    {
        // Providers that reuse the same tool-call index rely on the reasoning delta between two calls to
        // close out the first one; merging them glues the ids and produces invalid argument JSON.
        var sse = Wire.Stream(
            Wire.ToolCallDelta(index: 0, id: "call_a", name: "MyTool", arguments: "{}"),
            Wire.ReasoningDelta(RamblingReasoning),
            Wire.ToolCallDelta(index: 0, id: "call_b", name: "MyTool", arguments: "{}"),
            Wire.FinishChunk("tool_calls"));

        const string conversationId = "chats/toolcall-boundary";

        using var store = GetDocumentStore();
        var (_, error) = await StreamAsync(store, conversationId, InjectedResponse.Sse(sse), withTool: true);

        Assert.Null(error);

        using var session = store.OpenSession();
        var doc = session.Load<BlittableJsonReaderObject>(conversationId);
        Assert.NotNull(doc);
        var text = doc.ToString();
        Assert.DoesNotContain("call_acall_b", text);
        Assert.Contains("call_a", text);
        Assert.Contains("call_b", text);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Streaming_ServerError_AfterChunksWereSent_ReachesThePublicClientAsTheRealError()
    {
        // Full public-client path: the provider streams a valid prefix (so the HTTP response has started),
        // then truncates with finish_reason='length'. The server's exception is appended to the already-started
        // 200 response as the standard error envelope; the client must surface it, not parse it as the final
        // result.
        var sse = Wire.Stream(
            Wire.ContentDelta("{\"Answer\":\"partial answer "),
            Wire.FinishChunk("length"));

        using var provider = FakeSseProvider.Start(sse);
        using var store = GetDocumentStore();

        var connection = new AiConnectionString
        {
            Name = FakeConnectionStringName,
            Identifier = FakeConnectionStringName,
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings("fake-key", provider.Endpoint, "gpt-4o")
        };
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(connection));

        var agent = new AiAgentConfiguration("streaming-error-agent", FakeConnectionStringName, SystemPrompt);
        var identifier = (await store.AI.CreateAgentAsync(agent, new AnswerSchema())).Identifier;

        var chat = store.AI.Conversation(identifier, "chats/", new AiConversationCreationOptions());
        chat.SetUserPrompt(UserPrompt);

        var streamed = new StringBuilder();
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var error = await Assert.ThrowsAnyAsync<TooManyTokensException>(() => chat.StreamAsync<AnswerSchema>(x => x.Answer, chunk =>
        {
            streamed.Append(chunk);
            return Task.CompletedTask;
        }, cts.Token));

        // the chunks were delivered before the failure, and the real server error survives the started stream
        // rather than being parsed as the final result (which used to surface as a NullReferenceException)
        Assert.Contains("partial answer", streamed.ToString());
        Assert.DoesNotContain(nameof(NullReferenceException), error.ToString());
    }

    // A minimal OpenAI-compatible endpoint over a raw socket: answers every POST with the given SSE body.
    // Raw TCP avoids HttpListener's URL ACL requirements on Windows.
    private sealed class FakeSseProvider : IDisposable
    {
        private readonly System.Net.Sockets.TcpListener _listener;
        public string Endpoint { get; }

        private FakeSseProvider(System.Net.Sockets.TcpListener listener, string endpoint)
        {
            _listener = listener;
            Endpoint = endpoint;
        }

        public static FakeSseProvider Start(string sseBody)
        {
            var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
            listener.Start();
            var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
            var provider = new FakeSseProvider(listener, $"http://127.0.0.1:{port}/");
            _ = provider.ServeAsync(Encoding.UTF8.GetBytes(sseBody));
            return provider;
        }

        private async Task ServeAsync(byte[] body)
        {
            try
            {
                while (true)
                {
                    using var client = await _listener.AcceptTcpClientAsync();
                    var stream = client.GetStream();

                    // read the request headers, then the declared body length
                    var buffer = new byte[64 * 1024];
                    var read = 0;
                    int headerEnd;
                    while (true)
                    {
                        var n = await stream.ReadAsync(buffer, read, buffer.Length - read);
                        if (n == 0)
                            break;
                        read += n;
                        headerEnd = Encoding.ASCII.GetString(buffer, 0, read).IndexOf("\r\n\r\n", StringComparison.Ordinal);
                        if (headerEnd < 0)
                            continue;

                        var headers = Encoding.ASCII.GetString(buffer, 0, headerEnd);
                        var contentLength = 0;
                        foreach (var header in headers.Split("\r\n"))
                        {
                            if (header.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
                                contentLength = int.Parse(header.Substring("Content-Length:".Length).Trim());
                        }

                        var bodyRead = read - (headerEnd + 4);
                        while (bodyRead < contentLength)
                        {
                            var m = await stream.ReadAsync(buffer, 0, buffer.Length);
                            if (m == 0)
                                break;
                            bodyRead += m;
                        }

                        break;
                    }

                    var response = Encoding.ASCII.GetBytes(
                        "HTTP/1.1 200 OK\r\n" +
                        "Content-Type: text/event-stream\r\n" +
                        $"Content-Length: {body.Length}\r\n" +
                        "Connection: close\r\n\r\n");
                    await stream.WriteAsync(response);
                    await stream.WriteAsync(body);
                    await stream.FlushAsync();
                }
            }
            catch (Exception)
            {
                // listener disposed - test is done
            }
        }

        public void Dispose() => _listener.Stop();
    }

    #endregion

    #region non-streaming

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("empty-content-with-reasoning")]
    [InlineData("null-content-with-reasoning")]
    [InlineData("non-json-content")]
    [InlineData("incomplete-json-content")]
    [InlineData("json-array-content")]
    [InlineData("invalid-content-with-json-reasoning")]
    public async Task Structured_UnusableAnswer_ThrowsCleanError_WithoutLeakingTheParserError(string shape)
    {
        var response = shape switch
        {
            // the reported failure: the answer is empty and only chain-of-thought came back
            "empty-content-with-reasoning" => Wire.Completion("", finishReason: "stop", reasoning: RamblingReasoning),
            "null-content-with-reasoning" => Wire.Completion(null, finishReason: "stop", reasoning: RamblingReasoning),
            // the model answered in prose instead of JSON
            "non-json-content" => Wire.Completion("Sure! Here is the answer.", finishReason: "stop"),
            // the JSON object never closes - the parser reports it as an end-of-stream, not invalid data
            "incomplete-json-content" => Wire.Completion("{\"Answer\":\"x", finishReason: "stop"),
            // valid JSON, but an array where the schema requires an object
            "json-array-content" => Wire.Completion("[{\"Answer\":\"ok\"}]", finishReason: "stop"),
            // real content is authoritative even when invalid - the valid JSON reasoning must not be used
            "invalid-content-with-json-reasoning" => Wire.Completion("oops not json", finishReason: "stop", reasoning: "{\"Answer\":\"42\"}"),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "unknown response shape")
        };

        // tools stay enabled, so this is a plain answering turn
        var error = await RunAsync("chats/invalid-" + shape,
            InjectedResponse.Json(response), maxModelIterationsPerCall: 16, withTool: true);

        AssertNoParserErrorLeaked<UnexpectedResponseException>(error);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Structured_NonStreaming_CompletedAnswer_IsKept_DespiteTrailingGarbage()
    {
        // The non-streaming twin of the trailing-garbage streaming test: the completed structured answer wins
        // and the trailing text is dropped, matching the streaming path, so the two cannot silently diverge.
        const string content = "{\"Answer\":\"42\"} The answer is 42, as requested.";
        const string garbage = "as requested";
        const string conversationId = "chats/nonstream-trailing-text";

        using var store = GetDocumentStore();
        var error = await RunAsync(store, conversationId,
            InjectedResponse.Json(Wire.Completion(content, finishReason: "stop")), maxModelIterationsPerCall: 16, withTool: true);

        Assert.Null(error);

        // the persisted assistant answer is the parsed object, not the raw model text: '42' survived, the
        // garbage did not
        var messages = await store.AI.GetConversationMessagesAsync(conversationId);
        var assistant = messages.Messages.Find(m => m.Role == AiMessageRole.Assistant && m.Content != null);
        Assert.NotNull(assistant);
        Assert.Contains("42", assistant.Content);
        Assert.DoesNotContain(garbage, assistant.Content);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("json-in-reasoning")]
    [InlineData("json-in-reasoning-content")]
    public async Task Structured_NonStreaming_JsonReasoning_IsAcceptedAsFallback(string shape)
    {
        // RavenDB-25681: with no 'content' at all, the structured answer may come from
        // 'reasoning_content' / 'reasoning'
        var response = Wire.Completion("", finishReason: "stop", reasoning: "{\"Answer\":\"42\"}",
            reasoningContentField: shape == "json-in-reasoning-content");

        var conversationId = "chats/nonstream-reasoning-" + shape;

        using var store = GetDocumentStore();
        var error = await RunAsync(store, conversationId,
            InjectedResponse.Json(response), maxModelIterationsPerCall: 16, withTool: true);

        Assert.Null(error);

        var messages = await store.AI.GetConversationMessagesAsync(conversationId);
        var assistant = messages.Messages.Find(m => m.Role == AiMessageRole.Assistant && m.Content != null);
        Assert.NotNull(assistant);
        Assert.Contains("42", assistant.Content);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Structured_EmptyContentAndLength_ThrowsTooManyTokens()
    {
        // Truncated before any answer: the length-specific error must be raised rather than the generic
        // structured-output error, because retrying the same request cannot help.
        var error = await RunAsync("chats/length-empty",
            InjectedResponse.Json(Wire.Completion("", finishReason: "length", reasoning: RamblingReasoning)),
            maxModelIterationsPerCall: 16, withTool: true);

        Assert.True(HasException<TooManyTokensException>(error), $"expected TooManyTokensException, got: {error}");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Unstructured_EmptyContent_FallsBackToReasoning()
    {
        // RavenDB-25681: with no schema the reasoning text becomes the answer as-is. Structured output uses
        // the same fallback, but only accepts it when the complete reasoning forms the requested JSON object.
        const string conversationId = "chats/unstructured-reasoning";
        const string answer = "the plain text answer";

        using var store = GetDocumentStore();
        var error = await RunAsync(store, conversationId,
            InjectedResponse.Json(Wire.Completion("", finishReason: "stop", reasoning: answer)),
            maxModelIterationsPerCall: 16, withTool: true, noSchema: true);

        Assert.Null(error);

        using var session = store.OpenSession();
        var doc = session.Load<BlittableJsonReaderObject>(conversationId);
        Assert.NotNull(doc);
        Assert.Contains(answer, doc.ToString());
    }

    #endregion


    private static void AssertNoParserErrorLeaked<T>(Exception error) where T : Exception
    {
        Assert.True(HasException<T>(error), $"expected {typeof(T).Name}, got: {error}");

        // the raw JSON-parser error must never reach the caller
        var text = error.ToString();
        Assert.DoesNotContain("Cannot have a", text);
        Assert.DoesNotContain(nameof(InvalidDataException), text);
        Assert.DoesNotContain("InvalidStartOfObjectException", text);
        Assert.DoesNotContain(nameof(EndOfStreamException), text);
    }

    // No existing test helper walks an exception chain by type (ExtractSingleInnerException only unwraps a
    // single AggregateException), so the walk lives here.
    private static bool HasException<T>(Exception e) where T : Exception
    {
        for (; e != null; e = e.InnerException)
            if (e is T)
                return true;
        return false;
    }

    private static AiAgentConfiguration Agent(string connectionStringName, bool withTool) =>
        new("ravendb-26944-agent", connectionStringName, SystemPrompt)
        {
            Identifier = "ravendb-26944-agent",
            SampleObject = "{\"Answer\":\"a short answer\"}",
            // a real action, so a turn with iterations left genuinely offers tools to the model
            Actions = withTool
                ? [new AiAgentToolAction { Name = "MyTool", Description = "Returns an integer", ParametersSampleObject = "{}" }]
                : null
        };

    private InjectingConversationHandler CreateHandler(Raven.Server.Documents.DocumentDatabase database, InjectedResponse injected) =>
        new(Server.ServerStore, database, FakeConnection(), injected) { Authentication = null };

    private static void Initialize(InjectingConversationHandler handler, string conversationId,
        int maxModelIterationsPerCall, bool withTool = false, bool noSchema = false) =>
        handler.Initialize(Agent(FakeConnectionStringName, withTool), conversationId, new RequestBody
        {
            Parameters = null,
            CreationOptions = new AiConversationCreationOptions { MaxModelIterationsPerCall = maxModelIterationsPerCall },
            UserPrompt = UserPrompt,
            OutputOptions = noSchema ? new AiServerOutputOptions { NoSchema = true } : null
        }, changeVector: null);

    private async Task<Exception> RunAsync(
        string conversationId, InjectedResponse injected, int maxModelIterationsPerCall = 0, bool withTool = false)
    {
        using var store = GetDocumentStore();
        return await RunAsync(store, conversationId, injected, maxModelIterationsPerCall, withTool);
    }

    private async Task<Exception> RunAsync(
        IDocumentStore store, string conversationId,
        InjectedResponse injected, int maxModelIterationsPerCall = 0, bool withTool = false, bool noSchema = false)
    {
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(FakeConnection()));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = CreateHandler(database, injected);
            Initialize(handler, conversationId, maxModelIterationsPerCall, withTool, noSchema);

            try
            {
                await handler.HandleRequestAsync(context, CancellationToken.None);
            }
            catch (Exception e)
            {
                return e;
            }

            return null;
        }
    }

    private async Task<(string streamed, Exception error)> StreamAsync(
        IDocumentStore store, string conversationId,
        InjectedResponse injected, bool noSchema = false, bool withTool = false)
    {
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(FakeConnection()));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = CreateHandler(database, injected);
            // tool iterations stay enabled, so this stays a plain answer turn
            Initialize(handler, conversationId, maxModelIterationsPerCall: 16, withTool: withTool, noSchema: noSchema);

            // the real server-side streaming path writes the streamed property to this stream
            using var clientStream = new MemoryStream();

            Exception error = null;
            try
            {
                await handler.HandleStreamingRequestAsync(context, clientStream,
                    noSchema ? string.Empty : AnswerProperty, CancellationToken.None);
            }
            catch (Exception e)
            {
                error = e;
            }

            return (Encoding.UTF8.GetString(clientStream.ToArray()), error);
        }
    }
}
