using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Sparrow.Json;

namespace SlowTests.Server.Documents.AI.AiAgent;

/// <summary>
/// A conversation handler that uses a mock LLM instead of a real one.
/// </summary>
internal class MockLlmConversationHandler(
    Raven.Server.ServerWide.ServerStore server,
    DocumentDatabase database,
    Func<JObject, HttpResponseMessage> onRequest = null,
    Func<JObject, string, HttpResponseMessage> onToolResult = null,
    AbstractChatCompletionProvider clientSettings = null)
    : ConversationHandler(server, database)
{
    private readonly DocumentDatabase _database = database;

    internal MockLlm LastClient { get; private set; }

    protected internal override ChatCompletionClient CreateClient()
    {
        var settings = clientSettings ?? new OpenAiChatCompletionProvider(new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o"));
        LastClient = new MockLlm(_database.DocumentsStorage.ContextPool, settings, onRequest, onToolResult, ChatCompletionClient.ConventionsToUse);

        // Armed here - a test only gets the client through LastClient after the call is over.
        LastClient.ForTestingPurposesOnly();

        return LastClient;
    }
}

/// <summary>
/// A mock ChatCompletionClient that intercepts HTTP requests and returns predetermined responses.
/// <para>
/// The request handling pipeline is:
/// 1. <c>onRequest</c> is called with the full payload — return non-null to short-circuit.
/// 2. If any message has role "tool", <c>onToolResult</c> is called with the payload and tool content.
///    Default: echoes the tool content back as the answer.
/// 3. Falls through to a simple "mock response" answer.
/// </para>
/// </summary>
internal class MockLlm : ChatCompletionClient
{
    private readonly Func<JObject, HttpResponseMessage> _onRequest;
    private readonly Func<JObject, string, HttpResponseMessage> _onToolResult;

    internal MockLlm(
        IMemoryContextPool contextPool,
        AbstractChatCompletionProvider settings,
        Func<JObject, HttpResponseMessage> onRequest = null,
        Func<JObject, string, HttpResponseMessage> onToolResult = null,
        DocumentConventions conventions = null)
        : base(contextPool, settings, conventions)
    {
        _onRequest = onRequest;
        _onToolResult = onToolResult;
    }

    protected override async Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token)
    {
        var body = await request.Content!.ReadAsStringAsync(token);
        var payload = JObject.Parse(body);

        var response = _onRequest?.Invoke(payload);
        if (response != null)
            return response;

        foreach (var msg in payload["messages"])
        {
            if (msg["role"].ToString() == "tool")
            {
                var toolContent = msg["content"].ToString();
                if (_onToolResult != null)
                    return _onToolResult(payload, toolContent);

                return Ok(CreateAnswerResponse(toolContent));
            }
        }

        return Ok(CreateAnswerResponse("\"mock response\""));
    }

    private static HttpResponseMessage Ok(string content) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(content)
    };

    private static string UsageJson(int promptTokens) => $$"""
        "usage": {
            "prompt_tokens": {{promptTokens}},
            "completion_tokens": 10,
            "total_tokens": {{promptTokens + 10}},
            "prompt_tokens_details": {
                "cached_tokens": 0,
                "audio_tokens": 0
            },
            "completion_tokens_details": {
                "reasoning_tokens": 0,
                "audio_tokens": 0,
                "accepted_prediction_tokens": 0,
                "rejected_prediction_tokens": 0
            }
        }
        """;

    /// <summary>
    /// Creates a mock tool call response that instructs the agent to call the specified tool.
    /// </summary>
    public static string CreateToolCallResponse(string toolName, string arguments = "{}", int promptTokens = 100)
    {
        var escapedArgs = arguments.Replace("\"", "\\\"");
        return $$"""
            {
                "id": "chatcmpl-mock",
                "object": "chat.completion",
                "created": 1754549498,
                "model": "gpt-4o-2024-08-06",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [{
                            "id": "call_mock",
                            "type": "function",
                            "function": {
                                "name": "{{toolName}}",
                                "arguments": "{{escapedArgs}}"
                            }
                        }],
                        "refusal": null,
                        "annotations": []
                    },
                    "logprobs": null,
                    "finish_reason": "tool_calls"
                }],
                {{UsageJson(promptTokens)}},
                "service_tier": "default",
                "system_fingerprint": "fp_mock"
            }
            """;
    }

    /// <summary>
    /// Creates a mock tool call response with multiple tool calls.
    /// </summary>
    public static string CreateMultipleToolCallsResponse(int promptTokens = 100, params (string toolName, string arguments)[] tools)
    {
        var toolCalls = string.Join(",\n", Array.ConvertAll(tools, t =>
        {
            var escapedArgs = t.arguments.Replace("\"", "\\\"");
            return $$"""
                        {
                            "id": "call_mock_{{t.toolName}}",
                            "type": "function",
                            "function": {
                                "name": "{{t.toolName}}",
                                "arguments": "{{escapedArgs}}"
                            }
                        }
                """;
        }));

        return $$"""
            {
                "id": "chatcmpl-mock",
                "object": "chat.completion",
                "created": 1754549498,
                "model": "gpt-4o-2024-08-06",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [{{toolCalls}}],
                        "refusal": null,
                        "annotations": []
                    },
                    "logprobs": null,
                    "finish_reason": "tool_calls"
                }],
                {{UsageJson(promptTokens)}},
                "service_tier": "default",
                "system_fingerprint": "fp_mock"
            }
            """;
    }

    /// <summary>
    /// Creates a mock answer response (no tool calls).
    /// </summary>
    public static string CreateAnswerResponse(string content, int promptTokens = 100)
    {
        var escapedContent = content.Replace("\\", "\\\\").Replace("\"", "\\\"");
        return $$"""
            {
                "id": "chatcmpl-mock",
                "object": "chat.completion",
                "created": 1754549498,
                "model": "gpt-4o-2024-08-06",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "{\"Answer\":{{escapedContent}}}",
                        "refusal": null,
                        "annotations": []
                    },
                    "logprobs": null,
                    "finish_reason": "done"
                }],
                {{UsageJson(promptTokens)}},
                "service_tier": "default",
                "system_fingerprint": "fp_mock"
            }
            """;
    }
}

/// <summary>
/// A provider response injected in-process instead of being fetched from the real provider.
/// </summary>
internal sealed class InjectedResponse
{
    private InjectedResponse(string body, string contentType)
    {
        Body = body;
        ContentType = contentType;
    }

    public string Body { get; }
    public string ContentType { get; }

    /// <summary>A non-streaming chat completion body.</summary>
    public static InjectedResponse Json(string body) => new(body, "application/json");

    /// <summary>A streaming (SSE) response body, as the provider would send it.</summary>
    public static InjectedResponse Sse(string body) => new(body, "text/event-stream");
}

/// <summary>
/// Builders for provider wire shapes a real model cannot be asked to produce on demand (empty answers,
/// non-JSON answers, truncated answers). Text is JSON-escaped here, so callers pass plain strings.
/// </summary>
internal static class Wire
{
    private const int PromptTokens = 7000;
    private const int CompletionTokens = 777;
    private const int TotalTokens = PromptTokens + CompletionTokens;

    /// <summary>A non-streaming chat completion. A null <paramref name="content"/> means a JSON null content.</summary>
    public static string Completion(string content, string finishReason, string reasoning = null, bool reasoningContentField = false)
    {
        var contentJson = content == null ? "null" : JsonConvert.ToString(content);
        var reasoningJson = reasoning == null ? "" : $",\"{(reasoningContentField ? "reasoning_content" : "reasoning")}\":{JsonConvert.ToString(reasoning)}";
        return $$"""
        {
            "id": "chatcmpl-injected",
            "object": "chat.completion",
            "choices": [{
                "index": 0,
                "finish_reason": "{{finishReason}}",
                "message": { "role": "assistant", "content": {{contentJson}}{{reasoningJson}} }
            }],
            "usage": { "prompt_tokens": {{PromptTokens}}, "completion_tokens": {{CompletionTokens}}, "total_tokens": {{TotalTokens}} }
        }
        """;
    }

    /// <summary>Wraps SSE chunks into a full event stream, terminated the way providers terminate it.</summary>
    public static string Stream(params string[] chunks) =>
        string.Concat(Array.ConvertAll(chunks, c => "data: " + c + "\n\n")) + "data: [DONE]\n\n";

    /// <summary>An SSE chunk carrying a 'content' delta.</summary>
    public static string ContentDelta(string content) => Chunk($"{{\"content\":{JsonConvert.ToString(content)}}}");

    /// <summary>An SSE chunk carrying a 'reasoning' (or 'reasoning_content') delta.</summary>
    public static string ReasoningDelta(string reasoning, bool reasoningContentField = false) =>
        Chunk($"{{\"{(reasoningContentField ? "reasoning_content" : "reasoning")}\":{JsonConvert.ToString(reasoning)}}}");

    /// <summary>The terminating SSE chunk carrying finish_reason and usage.</summary>
    public static string FinishChunk(string finishReason) => Chunk("{}", finishReason, usage: true);

    /// <summary>An SSE chunk carrying one tool-call fragment. Providers may reuse the same index for
    /// consecutive calls, so the index is explicit.</summary>
    public static string ToolCallDelta(int index, string id, string name, string arguments) =>
        Chunk($"{{\"tool_calls\":[{{\"index\":{index},\"id\":{JsonConvert.ToString(id)},\"type\":\"function\",\"function\":{{\"name\":{JsonConvert.ToString(name)},\"arguments\":{JsonConvert.ToString(arguments)}}}}}]}}");

    private static string Chunk(string delta, string finishReason = null, bool usage = false)
    {
        var finish = finishReason == null ? "null" : $"\"{finishReason}\"";
        var usageJson = usage
            ? $",\"usage\":{{\"prompt_tokens\":{PromptTokens},\"completion_tokens\":{CompletionTokens},\"total_tokens\":{TotalTokens}}}"
            : "";
        return $"{{\"choices\":[{{\"index\":0,\"delta\":{delta},\"finish_reason\":{finish}}}]{usageJson}}}";
    }
}

/// <summary>
/// Builders for OpenAI Responses API (/responses) wire shapes. Text is JSON-escaped here, so callers pass plain strings.
/// </summary>
internal static class ResponsesWire
{
    public const int InputTokens = 7000;
    public const int OutputTokens = 777;
    public const int TotalTokens = 7800; // deliberately not input + output, so a test can tell the reported total from a computed one
    public const int CachedTokens = 300;
    public const int ReasoningTokens = 50;

    private static readonly string Usage =
        $"{{\"input_tokens\":{InputTokens},\"input_tokens_details\":{{\"cached_tokens\":{CachedTokens}}}," +
        $"\"output_tokens\":{OutputTokens},\"output_tokens_details\":{{\"reasoning_tokens\":{ReasoningTokens}}},\"total_tokens\":{TotalTokens}}}";

    /// <summary>A non-streaming response with the given output items.</summary>
    public static string Response(string status, params string[] output) => Build(status, incompleteReason: null, error: null, output);

    /// <summary>A non-streaming response with status "incomplete" and the given reason.</summary>
    public static string IncompleteResponse(string reason, params string[] output) => Build("incomplete", reason, error: null, output);

    /// <summary>A non-streaming response with status "failed".</summary>
    public static string FailedResponse(string code, string message) => Build("failed", incompleteReason: null, Error(code, message), []);

    private static string Build(string status, string incompleteReason, string error, string[] output)
    {
        var details = incompleteReason == null ? "null" : $"{{\"reason\":{JsonConvert.ToString(incompleteReason)}}}";
        return $"{{\"id\":\"resp_mock\",\"object\":\"response\",\"model\":\"gpt-5.4\",\"status\":{JsonConvert.ToString(status)}," +
               $"\"incomplete_details\":{details},\"error\":{error ?? "null"},\"output\":[{string.Join(",", output)}],\"usage\":{Usage}}}";
    }

    private static string Error(string code, string message) =>
        $"{{\"code\":{JsonConvert.ToString(code)},\"message\":{JsonConvert.ToString(message)}}}";

    public static string TextMessage(string text) =>
        $"{{\"type\":\"message\",\"id\":\"msg_mock\",\"status\":\"completed\",\"role\":\"assistant\"," +
        $"\"content\":[{{\"type\":\"output_text\",\"text\":{JsonConvert.ToString(text)},\"annotations\":[]}}]}}";

    public static string RefusalMessage(string refusal) =>
        $"{{\"type\":\"message\",\"id\":\"msg_mock\",\"status\":\"completed\",\"role\":\"assistant\"," +
        $"\"content\":[{{\"type\":\"refusal\",\"refusal\":{JsonConvert.ToString(refusal)}}}]}}";

    public static string Reasoning(string summary) =>
        $"{{\"type\":\"reasoning\",\"id\":\"rs_mock\",\"summary\":[{{\"type\":\"summary_text\",\"text\":{JsonConvert.ToString(summary)}}}]}}";

    public static string FunctionCall(string callId, string name, string arguments) =>
        $"{{\"type\":\"function_call\",\"id\":\"fc_{callId}\",\"call_id\":{JsonConvert.ToString(callId)},\"name\":{JsonConvert.ToString(name)}," +
        $"\"arguments\":{JsonConvert.ToString(arguments)},\"status\":\"completed\"}}";

    /// <summary>
    /// The text of one JSON property, <c>"content_filters":[...]</c>, without surrounding braces, so a builder can splice it into a response object.
    /// Has a "prompt" and a "completion" entry, shaped like Azure's: a blocked one has blocked:true and violence filtered with severity high.
    /// </summary>
    public static string AzureContentFilters(bool promptBlocked, bool completionBlocked) =>
        $"\"content_filters\":[{AzureContentFilter("prompt", promptBlocked)},{AzureContentFilter("completion", completionBlocked)}]";

    /// <summary>
    /// A "content_filters" property with one "completion" entry that is blocked but flags no category, so the entry carries no category text.
    /// </summary>
    public static string AzureContentFiltersBlockedWithoutCategory() =>
        "\"content_filters\":[{\"blocked\":true,\"source_type\":\"completion\",\"content_filter_results\":" +
        "{\"hate\":{\"filtered\":false,\"severity\":\"safe\"},\"self_harm\":{\"filtered\":false,\"severity\":\"safe\"}," +
        "\"sexual\":{\"filtered\":false,\"severity\":\"safe\"},\"violence\":{\"filtered\":false,\"severity\":\"safe\"}}}]";

    /// <summary>A non-streaming response as <see cref="Response"/>, with <paramref name="extraProperty"/> (the text of one JSON property) added at the top level.</summary>
    public static string ResponseWithExtra(string status, string extraProperty, params string[] output) =>
        InsertProperty(Build(status, incompleteReason: null, error: null, output), extraProperty);

    /// <summary>A response.created event whose "response" carries <paramref name="extraProperty"/>.</summary>
    public static string CreatedEvent(string extraProperty) =>
        $"{{\"type\":\"response.created\",\"response\":{{\"id\":\"resp_mock\",\"status\":\"in_progress\",\"output\":[],{extraProperty}}}}}";

    /// <summary>A response.completed event whose "response" carries <paramref name="extraProperty"/>.</summary>
    public static string CompletedEvent(string extraProperty) =>
        $"{{\"type\":\"response.completed\",\"response\":{{\"id\":\"resp_mock\",\"status\":\"completed\",\"output\":[],\"usage\":{Usage},{extraProperty}}}}}";

    /// <summary>A response.incomplete event whose "response" carries <paramref name="extraProperty"/>.</summary>
    public static string IncompleteEvent(string reason, string extraProperty) =>
        $"{{\"type\":\"response.incomplete\",\"response\":{{\"id\":\"resp_mock\",\"status\":\"incomplete\"," +
        $"\"incomplete_details\":{{\"reason\":{JsonConvert.ToString(reason)}}},\"output\":[],\"usage\":{Usage},{extraProperty}}}}}";

    private static string InsertProperty(string json, string property) => "{" + property + "," + json.Substring(1);

    private static string AzureContentFilter(string sourceType, bool blocked)
    {
        var violence = blocked ? "{\"filtered\":true,\"severity\":\"high\"}" : "{\"filtered\":false,\"severity\":\"safe\"}";
        return $"{{\"blocked\":{(blocked ? "true" : "false")},\"source_type\":\"{sourceType}\",\"content_filter_results\":" +
               "{\"hate\":{\"filtered\":false,\"severity\":\"safe\"},\"self_harm\":{\"filtered\":false,\"severity\":\"safe\"}," +
               $"\"sexual\":{{\"filtered\":false,\"severity\":\"safe\"}},\"violence\":{violence}}}}}";
    }

    /// <summary>A full SSE stream: each event as an "event:" line plus a "data:" line. Responses streams have no [DONE].</summary>
    public static string Stream(params string[] events) =>
        string.Concat(Array.ConvertAll(events, e => "event: " + JObject.Parse(e)["type"] + "\ndata: " + e + "\n\n"));

    public static string TextDelta(string delta) =>
        $"{{\"type\":\"response.output_text.delta\",\"item_id\":\"msg_mock\",\"output_index\":0,\"content_index\":0,\"delta\":{JsonConvert.ToString(delta)}}}";

    public static string RefusalDelta(string delta) =>
        $"{{\"type\":\"response.refusal.delta\",\"item_id\":\"msg_mock\",\"output_index\":0,\"content_index\":0,\"delta\":{JsonConvert.ToString(delta)}}}";

    public static string FunctionCallAdded(string itemId, string callId, string name) =>
        $"{{\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{{\"type\":\"function_call\",\"id\":{JsonConvert.ToString(itemId)}," +
        $"\"call_id\":{JsonConvert.ToString(callId)},\"name\":{JsonConvert.ToString(name)},\"arguments\":\"\",\"status\":\"in_progress\"}}}}";

    public static string ArgumentsDelta(string itemId, string delta) =>
        $"{{\"type\":\"response.function_call_arguments.delta\",\"item_id\":{JsonConvert.ToString(itemId)},\"output_index\":0,\"delta\":{JsonConvert.ToString(delta)}}}";

    public static string FunctionCallDone(string itemId, string callId, string name, string arguments) =>
        $"{{\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{{\"type\":\"function_call\",\"id\":{JsonConvert.ToString(itemId)}," +
        $"\"call_id\":{JsonConvert.ToString(callId)},\"name\":{JsonConvert.ToString(name)},\"arguments\":{JsonConvert.ToString(arguments)},\"status\":\"completed\"}}}}";

    public static string Completed() =>
        $"{{\"type\":\"response.completed\",\"response\":{{\"id\":\"resp_mock\",\"status\":\"completed\",\"output\":[],\"usage\":{Usage}}}}}";

    public static string Incomplete(string reason) =>
        $"{{\"type\":\"response.incomplete\",\"response\":{{\"id\":\"resp_mock\",\"status\":\"incomplete\"," +
        $"\"incomplete_details\":{{\"reason\":{JsonConvert.ToString(reason)}}},\"output\":[],\"usage\":{Usage}}}}}";

    public static string Failed(string code, string message) =>
        $"{{\"type\":\"response.failed\",\"response\":{{\"id\":\"resp_mock\",\"status\":\"failed\",\"error\":{Error(code, message)},\"output\":[],\"usage\":{Usage}}}}}";

    public static string StreamError(string code, string message) =>
        $"{{\"type\":\"error\",\"code\":{JsonConvert.ToString(code)},\"message\":{JsonConvert.ToString(message)},\"param\":null}}";
}

/// <summary>
/// A real <see cref="ConversationHandler"/> whose provider response is supplied in-process instead of over
/// HTTP, for responses a real model cannot be asked to produce on demand. Unlike <see cref="MockLlm"/> this
/// also serves the streaming send, so SSE shapes can be injected.
/// </summary>
internal class InjectingConversationHandler(
    Raven.Server.ServerWide.ServerStore server,
    DocumentDatabase database,
    AiConnectionString connection,
    InjectedResponse injected)
    : ConversationHandler(server, database)
{
    private readonly DocumentDatabase _database = database;
    private ChatCompletionClient _client;

    protected internal override ChatCompletionClient CreateClient()
    {
        if (_client != null)
            return _client;

        if (AbstractChatCompletionProvider.TryGetParameters(connection, out var settings) == false)
            throw new NotSupportedException($"The provider '{connection.GetActiveProvider()}' is not supported.");

        return _client = new InjectingClient(_database.DocumentsStorage.ContextPool, settings, injected);
    }

    private sealed class InjectingClient(
        IMemoryContextPool contextPool,
        AbstractChatCompletionProvider settings,
        InjectedResponse injected)
        : ChatCompletionClient(contextPool, settings, ConventionsToUse)
    {
        protected override Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token) =>
            Task.FromResult(Injected());

        protected override Task<HttpResponseMessage> SendStreamingRequestAsync(HttpRequestMessage request, CancellationToken token) =>
            Task.FromResult(Injected());

        private HttpResponseMessage Injected() => new(HttpStatusCode.OK)
        {
            Content = new StringContent(injected.Body, Encoding.UTF8, injected.ContentType)
        };
    }
}
