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
// A thinking model can answer with an empty 'content' and a large 'reasoning' block. That reasoning is
// natural language, never the structured answer, so it must not be parsed as JSON - doing so was the
// RavenDB-26944 crash. Unstructured output keeps the reasoning fallback (RavenDB-25681), and a
// length-truncated response is incomplete and must fail with the length-specific error instead.
//
// The happy paths run fully live. The failure shapes are hybrid: the pipeline, the connection string and
// the provider client are real, and only the responses a real model cannot be asked to produce on demand
// (empty, non-JSON, or truncated answers) are injected - see InjectingConversationHandler in MockLlmHelper
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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["reasoning-only"])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["reasoning-content-only"])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["non-json-content"])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["incomplete-json-content"])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["json-array-content"])]
    public async Task Structured_UnusableStream_ThrowsCleanError_WithoutLeakingTheParserError(
        Options options, GenAiConfiguration config, string shape)
    {
        var sse = shape switch
        {
            // only chain-of-thought was streamed - it must never be parsed as the answer
            "reasoning-only" => Wire.Stream(Wire.ReasoningDelta(RamblingReasoning), Wire.FinishChunk("stop")),
            "reasoning-content-only" => Wire.Stream(Wire.ReasoningDelta(RamblingReasoning, reasoningContentField: true), Wire.FinishChunk("stop")),
            // the model streamed prose instead of JSON
            "non-json-content" => Wire.Stream(Wire.ContentDelta("Sure, here is the answer:"), Wire.FinishChunk("stop")),
            // the stream ended in the middle of the JSON object
            "incomplete-json-content" => Wire.Stream(Wire.ContentDelta("{\"Answer\":"), Wire.FinishChunk("stop")),
            // well-formed JSON, but an array root where the schema requires an object
            "json-array-content" => Wire.Stream(Wire.ContentDelta("[{\"Answer\":\"ok\"}]"), Wire.FinishChunk("stop")),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "unknown stream shape")
        };

        using var store = GetDocumentStore(options);
        var (_, error) = await StreamAsync(store, config, "chats/bad-stream-" + shape, InjectedResponse.Sse(sse));

        AssertNoParserErrorLeaked<UnexpectedResponseException>(error);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["no-content"])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["valid-content"])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["invalid-content-before-length"])]
    public async Task Structured_LengthTruncatedStream_AlwaysThrowsTooManyTokens(Options options, GenAiConfiguration config, string shape)
    {
        var sse = shape switch
        {
            "no-content" => Wire.Stream(Wire.ReasoningDelta(RamblingReasoning), Wire.FinishChunk("length")),
            // a complete-looking answer that was still cut off: it must not be accepted
            "valid-content" => Wire.Stream(Wire.ContentDelta("{\"Answer\":\"42\"}"), Wire.FinishChunk("length")),
            // the parser rejected the content mid-stream; the later 'length' signal must still be observed
            "invalid-content-before-length" => Wire.Stream(Wire.ContentDelta("{\"Answer\":"), Wire.ContentDelta("oops not json}"), Wire.FinishChunk("length")),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "unknown stream shape")
        };

        using var store = GetDocumentStore(options);
        var (_, error) = await StreamAsync(store, config, "chats/length-stream-" + shape, InjectedResponse.Sse(sse));

        Assert.True(HasException<TooManyTokensException>(error), $"expected TooManyTokensException, got: {error}");
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Structured_ValidPrefix_IsStreamedToClient_BeforeTheDeferredFailure(Options options, GenAiConfiguration config)
    {
        // The incremental parser rejects the content mid-stream, but the valid prefix was already delivered
        // to the client and must stay delivered; the failure is reported afterwards.
        var sse = Wire.Stream(
            Wire.ContentDelta("{\"Answer\":\"valid prefix "),
            Wire.ContentDelta("text\" garbage"),
            Wire.FinishChunk("stop"));

        using var store = GetDocumentStore(options);
        var (streamed, error) = await StreamAsync(store, config, "chats/prefix-then-invalid", InjectedResponse.Sse(sse));

        Assert.True(HasException<UnexpectedResponseException>(error), $"expected UnexpectedResponseException, got: {error}");
        Assert.Contains("valid prefix ", streamed);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = [false])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = [true])]
    public async Task Unstructured_ReasoningOnlyStream_FallsBackToReasoning(Options options, GenAiConfiguration config, bool reasoningContentField)
    {
        // RavenDB-25681: with no schema, a model that streams its answer only in 'reasoning' /
        // 'reasoning_content' is still usable and that text becomes the answer.
        const string answer = "the plain text answer";
        var sse = Wire.Stream(Wire.ReasoningDelta(answer, reasoningContentField), Wire.FinishChunk("stop"));

        using var store = GetDocumentStore(options);
        var (streamed, error) = await StreamAsync(store, config, $"chats/unstructured-stream-{reasoningContentField}",
            InjectedResponse.Sse(sse), noSchema: true);

        Assert.Null(error);
        Assert.Contains(answer, streamed);
    }

    #endregion

    #region non-streaming

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["empty-content-with-reasoning"])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["null-content-with-reasoning"])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["non-json-content"])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, Data = ["json-array-content"])]
    public async Task Structured_UnusableAnswer_ThrowsCleanError_WithoutLeakingTheParserError(
        Options options, GenAiConfiguration config, string shape)
    {
        var response = shape switch
        {
            // the reported failure: the answer is empty and only chain-of-thought came back
            "empty-content-with-reasoning" => Wire.Completion("", finishReason: "stop", reasoning: RamblingReasoning),
            "null-content-with-reasoning" => Wire.Completion(null, finishReason: "stop", reasoning: RamblingReasoning),
            // the model answered in prose instead of JSON
            "non-json-content" => Wire.Completion("Sure! Here is the answer.", finishReason: "stop"),
            // valid JSON, but an array where the schema requires an object
            "json-array-content" => Wire.Completion("[{\"Answer\":\"ok\"}]", finishReason: "stop"),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "unknown response shape")
        };

        // tools stay enabled, so this is a plain answering turn
        var error = await RunAsync(options, config, "chats/invalid-" + shape,
            InjectedResponse.Json(response), maxModelIterationsPerCall: 16, withTool: true);

        AssertNoParserErrorLeaked<UnexpectedResponseException>(error);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Structured_EmptyContentAndLength_ThrowsTooManyTokens(Options options, GenAiConfiguration config)
    {
        // Truncated before any answer: the length-specific error must be raised rather than the generic
        // structured-output error, because retrying the same request cannot help.
        var error = await RunAsync(options, config, "chats/length-empty",
            InjectedResponse.Json(Wire.Completion("", finishReason: "length", reasoning: RamblingReasoning)),
            maxModelIterationsPerCall: 16, withTool: true);

        Assert.True(HasException<TooManyTokensException>(error), $"expected TooManyTokensException, got: {error}");
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Unstructured_EmptyContent_FallsBackToReasoning(Options options, GenAiConfiguration config)
    {
        // RavenDB-25681: with no schema the reasoning text becomes the answer. Only structured output
        // refuses that fallback.
        const string conversationId = "chats/unstructured-reasoning";
        const string answer = "the plain text answer";

        using var store = GetDocumentStore(options);
        var error = await RunAsync(store, config, conversationId,
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

    private InjectingConversationHandler CreateHandler(Raven.Server.Documents.DocumentDatabase database, GenAiConfiguration config,
        InjectedResponse injected) =>
        new(Server.ServerStore, database, config.Connection, injected) { Authentication = null };

    private static void Initialize(InjectingConversationHandler handler, GenAiConfiguration config, string conversationId,
        int maxModelIterationsPerCall, bool withTool = false, bool noSchema = false) =>
        handler.Initialize(Agent(config.ConnectionStringName, withTool), conversationId, new RequestBody
        {
            Parameters = null,
            CreationOptions = new AiConversationCreationOptions { MaxModelIterationsPerCall = maxModelIterationsPerCall },
            UserPrompt = UserPrompt,
            OutputOptions = noSchema ? new AiServerOutputOptions { NoSchema = true } : null
        }, changeVector: null);

    private async Task<Exception> RunAsync(
        Options options, GenAiConfiguration config, string conversationId,
        InjectedResponse injected, int maxModelIterationsPerCall = 0, bool withTool = false)
    {
        using var store = GetDocumentStore(options);
        return await RunAsync(store, config, conversationId, injected, maxModelIterationsPerCall, withTool);
    }

    private async Task<Exception> RunAsync(
        IDocumentStore store, GenAiConfiguration config, string conversationId,
        InjectedResponse injected, int maxModelIterationsPerCall = 0, bool withTool = false, bool noSchema = false)
    {
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = CreateHandler(database, config, injected);
            Initialize(handler, config, conversationId, maxModelIterationsPerCall, withTool, noSchema);

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
        IDocumentStore store, GenAiConfiguration config, string conversationId,
        InjectedResponse injected, bool noSchema = false)
    {
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = CreateHandler(database, config, injected);
            // tool iterations stay enabled, so this stays a plain answer turn
            Initialize(handler, config, conversationId, maxModelIterationsPerCall: 16, noSchema: noSchema);

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
