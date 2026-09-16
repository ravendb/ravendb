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
    AbstractChatCompletionClientSettings clientSettings = null)
    : ConversationHandler(server, database)
{
    private readonly DocumentDatabase _database = database;

    protected internal override ChatCompletionClient CreateClient()
    {
        var settings = clientSettings ?? new OpenAiChatCompletionClientSettings(new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o"));
        return new MockLlm(_database.DocumentsStorage.ContextPool, settings, onRequest, onToolResult, ChatCompletionClient.ConventionsToUse);
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
        AbstractChatCompletionClientSettings settings,
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

        if (AbstractChatCompletionClientSettings.TryGetParameters(connection, out var settings) == false)
            throw new NotSupportedException($"The provider '{connection.GetActiveProvider()}' is not supported.");

        return _client = new InjectingClient(_database.DocumentsStorage.ContextPool, settings, injected);
    }

    private sealed class InjectingClient(
        IMemoryContextPool contextPool,
        AbstractChatCompletionClientSettings settings,
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
