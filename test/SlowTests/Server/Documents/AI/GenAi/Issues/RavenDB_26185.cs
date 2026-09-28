using System;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
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
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;
public class RavenDB_26185(ITestOutputHelper output) : RavenTestBase(output)
{
    // Built from a sample object, the same way GenAI configurations and the test connectivity probe define their output.
    private static readonly string AnswerSchema = ChatCompletionClient.GetSchemaFromSampleObject("{ \"Answer\": \"answer here\" }");

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task LiveProvider_Length_ThrowsTooManyTokensExceptionOnFinishReasonLength(Options options, GenAiConfiguration config)
    {
        await AssertLiveProviderLengthAsync(config, streaming: false);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task LiveProvider_StreamingLength_ThrowsTooManyTokensExceptionOnFinishReasonLength(Options options, GenAiConfiguration config)
    {
        await AssertLiveProviderLengthAsync(config, streaming: true);
    }

    // Gemini returned length without tool arguments at this cap; this scenario is covered on OpenAI.
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task LiveProvider_ToolCallLength_ThrowsTooManyTokensExceptionOnFinishReasonLength(Options options, GenAiConfiguration config)
    {
        await AssertLiveProviderToolCallLengthAsync(config, streaming: false);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task LiveProvider_StreamingToolCallLength_ThrowsTooManyTokensExceptionOnFinishReasonLength(Options options, GenAiConfiguration config)
    {
        await AssertLiveProviderToolCallLengthAsync(config, streaming: true);
    }

    private const string LiveLengthPrompt =
        "Copy the supplied text exactly into the JSON string property requested by the schema. " +
        "Include every word. Do not summarize, abbreviate, or use an ellipsis.";

    private static string LiveLengthInput => string.Join(" ", Enumerable.Repeat("The blue boat crossed the quiet lake.", 100));

    private static AbstractChatCompletionClientSettings CreateLiveLengthSettings(GenAiConfiguration config,
        LiveResponseObservation observation)
    {
        if (config.Connection.OpenAiSettings is { } openAi)
        {
            // Pin gpt-4o-mini: verified to produce the response shapes these tests need at the 16-token cap - a 200 with
            // finish_reason "length" on both transports, and tool-call arguments before the cut.
            // Copy connection details without mutating the supplied configuration.
            return new ObservedLiveOpenAiSettings(new OpenAiSettings(openAi.ApiKey, openAi.Endpoint, "gpt-4o-mini",
                organizationId: openAi.OrganizationId, projectId: openAi.ProjectId), observation);
        }

        if (config.Connection.GoogleSettings is { } google)
        {
            // This adapter calls /v1beta/openai/chat/completions (or the configured API version).
            // Keep the Gemini model supplied by the existing test connector.
            return new ObservedLiveGoogleSettings(new GoogleSettings(google.Model, google.ApiKey,
                endpoint: google.Endpoint, aiVersion: google.AiVersion), observation);
        }

        throw new InvalidOperationException("Live length tests support only OpenAI and Google OpenAI-compatible connections.");
    }

    private static async Task AssertLiveProviderLengthAsync(GenAiConfiguration config, bool streaming)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        using var pool = new JsonContextPool();
        var observation = new LiveResponseObservation();
        var settings = CreateLiveLengthSettings(config, observation);
        using var client = new TokenLimitedLiveClient(pool, settings, timeout.Token, maxRequests: 1);

        using (pool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            var system = ctx.ReadObject(new DynamicJsonValue
            {
                ["role"] = "system",
                ["content"] = LiveLengthPrompt
            }, "live-length/system");
            var user = ctx.ReadObject(new DynamicJsonValue
            {
                ["role"] = "user",
                ["content"] = LiveLengthInput
            }, "live-length/user");

            using var request = client.CreateCompletionRequest(ctx, [system, user], attachments: null,
                tools: null, useTools: false, streaming: streaming, schema: AnswerSchema);
            var usage = new AiUsage();

            await Assert.ThrowsAsync<TooManyTokensException>(async () =>
            {
                if (streaming)
                {
                    await client.StreamingCompleteAsync(ctx, pool, StreamProperty, request,
                        _ => Task.CompletedTask, usage, AnswerSchema, trace: null, token: timeout.Token);
                }
                else
                {
                    await client.CompleteAsync(ctx, request, usage, AnswerSchema, trace: null, token: timeout.Token);
                }
            });

            // Observe the actual provider signal independently of the exception's formatted message.
            Assert.Equal("length", observation.FinishReason);
            Assert.Equal(1, client.RequestCount);
        }
    }

    private const string LiveToolName = "record_text";

    private const string LiveToolCallPrompt =
        "Call the record_text tool. Put the supplied text, copied exactly and in full, into its 'text' argument. " +
        "Do not summarize, abbreviate, or use an ellipsis.";

    private static BlittableJsonReaderObject CreateLiveTool(JsonOperationContext ctx) => ctx.ReadObject(new DynamicJsonValue
    {
        ["type"] = "function",
        ["function"] = new DynamicJsonValue
        {
            ["name"] = LiveToolName,
            ["description"] = "Records a piece of text verbatim.",
            ["parameters"] = new DynamicJsonValue
            {
                ["type"] = "object",
                ["properties"] = new DynamicJsonValue { ["text"] = new DynamicJsonValue { ["type"] = "string" } },
                ["required"] = new DynamicJsonArray(new[] { "text" }),
                ["additionalProperties"] = false
            }
        }
    }, "live-tool");

    // The tool call is forced through tool_choice and the output capped, so the arguments are cut off. That must
    // surface as TooManyTokensException, not as a tool response carrying truncated argument JSON.
    private static async Task AssertLiveProviderToolCallLengthAsync(GenAiConfiguration config, bool streaming)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        using var pool = new JsonContextPool();
        var observation = new LiveResponseObservation();
        var settings = CreateLiveLengthSettings(config, observation);
        using var client = new TokenLimitedLiveClient(pool, settings, timeout.Token, maxRequests: 1, forcedTool: LiveToolName);

        using (pool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            var system = ctx.ReadObject(new DynamicJsonValue
            {
                ["role"] = "system",
                ["content"] = LiveToolCallPrompt
            }, "live-tool-length/system");
            var user = ctx.ReadObject(new DynamicJsonValue
            {
                ["role"] = "user",
                ["content"] = LiveLengthInput
            }, "live-tool-length/user");

            using var request = client.CreateCompletionRequest(ctx, [system, user], attachments: null,
                tools: [CreateLiveTool(ctx)], useTools: true, streaming: streaming, schema: null);
            var usage = new AiUsage();

            await Assert.ThrowsAsync<TooManyTokensException>(async () =>
            {
                if (streaming)
                {
                    await client.StreamingCompleteAsync(ctx, pool, StreamProperty, request,
                        _ => Task.CompletedTask, usage, schema: null, trace: null, token: timeout.Token);
                }
                else
                {
                    await client.CompleteAsync(ctx, request, usage, schema: null, trace: null, token: timeout.Token);
                }
            });

            Assert.Equal("length", observation.FinishReason);
            Assert.True(observation.SawToolCallArguments,
                $"The provider stopped on length before emitting any tool-call arguments, so this run did not exercise " +
                $"length handling with tool output present. HTTP status: {client.LastHttpStatus ?? "not observed"}.");
            Assert.Equal(1, client.RequestCount);
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task LiveProvider_LengthOnlyBatch_IsHashedWithoutPatchOrRefresh(Options options, GenAiConfiguration config)
    {
        // Shared budget for provider requests and hash/progress waits.
        var budget = TimeSpan.FromSeconds(90);
        var clock = Stopwatch.StartNew();
        using var timeout = new CancellationTokenSource(budget);
        int Remaining() => (int)Math.Max(0, (budget - clock.Elapsed).TotalMilliseconds);

        using var store = GetDocumentStore(options);
        var db = await GetDatabase(store.Database);
        var observation = new LiveResponseObservation();
        var settings = CreateLiveLengthSettings(config, observation);

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        // RavenGenAiData does not populate Identifier. Give the server and our metadata assertions the same key.
        config.Identifier = "live-length-26185";
        config.Collection = "Posts";
        config.Prompt = LiveLengthPrompt;
        config.SampleObject = JsonConvert.SerializeObject(new { Result = "text" });
        config.UpdateScript = @"const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].Result = $output.Result;";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = "for (const comment of this.Comments) ai.genContext({Text: comment.Text, Id: comment.Id});"
        };
        config.MaxConcurrency = 1;
        store.Maintenance.Send(new AddGenAiOperation(config));

        var etlProcess = await WaitForGenAiProcessAsync(db);
        // The ETL process owns this client. Its transport calls the real provider; the wrapper only limits requests.
        var client = new TokenLimitedLiveClient(db.DocumentsStorage.ContextPool, settings, timeout.Token, maxRequests: 1);
        var field = typeof(GenAiTask).GetField("_chatCompletionClient", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(field);
        var previous = (ChatCompletionClient)field.GetValue(etlProcess);
        field.SetValue(etlProcess, client);
        previous?.Dispose();

        const string docId = "posts/live-length";

        // Fresh database, so the first batch that loads anything is this document's. Subscribe before storing.
        var firstBatch = Etl.WaitForEtlToComplete(store);
        using (var session = store.OpenSession())
        {
            session.Store(new GenAiBasics.Post(
                [new GenAiBasics.Comment(LiveLengthInput, "author") { Id = "1" }], "t", "b"), docId);
            session.SaveChanges();
        }

        Assert.True(await firstBatch.WaitAsync(TimeSpan.FromMilliseconds(Remaining())),
            $"The ETL did not complete a batch for the document. Identifier: {config.Identifier}; " +
            $"request attempts: {client.RequestCount}; HTTP status: {client.LastHttpStatus ?? "not observed"}; " +
            $"finish reason: {observation.FinishReason ?? "not observed"}.");

        // The event only says the batch finished; the outcome is asserted explicitly. A real provider length failure must
        // be hashed even when it is the only item in the batch, with no @refresh and no applied update.
        Assert.True(await HasHashAsync(store, docId, config.Identifier),
            $"A real provider length failure must be hashed. Identifier: {config.Identifier}; " +
            $"request attempts: {client.RequestCount}; HTTP status: {client.LastHttpStatus ?? "not observed"}; " +
            $"finish reason: {observation.FinishReason ?? "not observed"}.");
        Assert.Equal("length", observation.FinishReason);
        Assert.False(await HasRefreshAsync(store, docId));
        Assert.False(await IsPatchedAsync(store, docId));
        Assert.Equal(1, client.RequestCount);

        // Change a field the transformation script does not read, so the generated context and its hash are unchanged.
        // The baseline is the document's etag right now; after it, only the patch below writes to this database, so the
        // ETL can only get past the baseline by loading the patched document.
        long baselineEtag;
        using (var session = store.OpenSession())
        {
            var post = session.Load<GenAiBasics.Post>(docId);
            baselineEtag = ChangeVectorUtils.GetEtagById(session.Advanced.GetChangeVectorFor(post), db.DbBase64Id);
        }

        var updateBatch = Etl.WaitForEtlToComplete(store, (_, statistics) => statistics.LastProcessedEtag > baselineEtag);
        using (var session = store.OpenSession())
        {
            session.Advanced.Patch<GenAiBasics.Post, string>(docId, x => x.Title, "changed outside the generated context");
            session.SaveChanges();
        }

        // LastProcessedEtag is assigned only after Load finished the batch, so passing the baseline means the update went
        // through the model step. A resend would also blow the one-request budget and leave the etag behind.
        Assert.True(await updateBatch.WaitAsync(TimeSpan.FromMilliseconds(Remaining())),
            $"The ETL did not process the metadata-only update. Request attempts: {client.RequestCount}; " +
            $"last processed etag: {etlProcess.Statistics.LastProcessedEtag}; baseline etag: {baselineEtag}.");
        Assert.Equal(1, client.RequestCount); // the unchanged context was not sent to the model again
    }

    private sealed class LiveResponseObservation
    {
        private string _finishReason;
        private bool _sawToolCallArguments;

        public string FinishReason => _finishReason;
        public bool SawToolCallArguments => _sawToolCallArguments;

        // Runs on every choice the client reads: once per non-streaming response, once per chunk when streaming.
        public string Observe(BlittableJsonReaderObject choice0, string reason)
        {
            if (string.IsNullOrEmpty(reason) == false)
                _finishReason = reason;

            if (HasToolCallArguments(choice0))
                _sawToolCallArguments = true;

            return reason;
        }

        // Non-streaming carries tool_calls on choice.message; streaming spreads argument fragments over choice.delta.
        private static bool HasToolCallArguments(BlittableJsonReaderObject choice0)
        {
            if (choice0 == null)
                return false;

            if ((choice0.TryGet(ChatCompletionClient.Constants.ResponseFields.Message, out BlittableJsonReaderObject carrier) == false || carrier == null)
                && (choice0.TryGet(ChatCompletionClient.Constants.ResponseFields.Delta, out carrier) == false || carrier == null))
                return false;

            if (carrier.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray calls) == false || calls == null)
                return false;

            foreach (BlittableJsonReaderObject call in calls)
            {
                if (call != null
                    && call.TryGet(ChatCompletionClient.Constants.ResponseFields.Function, out BlittableJsonReaderObject function)
                    && function != null
                    && function.TryGet(ChatCompletionClient.Constants.ResponseFields.Arguments, out object arguments)
                    && string.IsNullOrEmpty(arguments?.ToString()) == false)
                    return true;
            }

            return false;
        }
    }

    private sealed class ObservedLiveOpenAiSettings(OpenAiSettings settings, LiveResponseObservation observation)
        : OpenAiChatCompletionClientSettings(settings)
    {
        public override string GetFinishReason(BlittableJsonReaderObject choice0)
            => observation.Observe(choice0, base.GetFinishReason(choice0));
    }

    private sealed class ObservedLiveGoogleSettings(GoogleSettings settings, LiveResponseObservation observation)
        : GoogleChatCompletionClientSettings(settings)
    {
        public override string GetFinishReason(BlittableJsonReaderObject choice0)
            => observation.Observe(choice0, base.GetFinishReason(choice0));
    }

    private sealed class TokenLimitedLiveClient : ChatCompletionClient
    {
        private readonly CancellationToken _deadline;
        private readonly int _maxRequests;
        private readonly string _forcedTool;
        private int _requestCount;
        private string _lastHttpStatus;

        public int RequestCount => Volatile.Read(ref _requestCount);
        public string LastHttpStatus => _lastHttpStatus;

        public TokenLimitedLiveClient(IMemoryContextPool pool, AbstractChatCompletionClientSettings settings,
            CancellationToken deadline, int maxRequests, string forcedTool = null)
            : base(pool, settings, ConventionsToUse)
        {
            _deadline = deadline;
            _maxRequests = maxRequests;
            _forcedTool = forcedTool;
        }

        protected override async Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, _deadline);
            await LimitOutputAsync(request, linked.Token);
            var response = await base.SendRequestAsync(request, linked.Token);
            _lastHttpStatus = ((int)response.StatusCode).ToString();
            return response;
        }

        protected override async Task<HttpResponseMessage> SendStreamingRequestAsync(HttpRequestMessage request, CancellationToken token)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, _deadline);
            await LimitOutputAsync(request, linked.Token);
            // The caller also passes the deadline token to StreamingCompleteAsync, covering subsequent body reads.
            var response = await base.SendStreamingRequestAsync(request, linked.Token);
            _lastHttpStatus = ((int)response.StatusCode).ToString();
            return response;
        }

        private async Task LimitOutputAsync(HttpRequestMessage request, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (Interlocked.Increment(ref _requestCount) > _maxRequests)
                throw new InvalidOperationException("Live length test exceeded its provider-request budget.");

            var original = request.Content;
            var payload = JObject.Parse(await original.ReadAsStringAsync(token));
            payload.Remove("max_tokens");
            payload["max_completion_tokens"] = 16;
            if (_forcedTool != null)
                payload["tool_choice"] = new JObject { ["type"] = "function", ["function"] = new JObject { ["name"] = _forcedTool } };
            request.Content = new StringContent(payload.ToString(Formatting.None), Encoding.UTF8, "application/json");
            original.Dispose();
        }
    }

    private const string StreamProperty = "Answer";

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
