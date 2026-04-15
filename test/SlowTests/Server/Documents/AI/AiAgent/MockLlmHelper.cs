using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
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
