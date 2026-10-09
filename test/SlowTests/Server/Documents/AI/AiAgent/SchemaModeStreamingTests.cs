using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Tests.Infrastructure;
using Voron;
using Xunit;
using static SlowTests.Server.Documents.AI.CapturingChatCompletionClient;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class SchemaModeStreamingTests : RavenTestBase
    {
        public SchemaModeStreamingTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string AnswerPath = "Answer";

        // ---- Anthropic wire -------------------------------------------------------------------------------------

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Anthropic_Streaming_WithoutSchema_PlainTextThenToolUse_YieldsToolResponse()
        {
            var sse = AnthropicSse(textBlocks: ["let me check that"], toolUse: ("toolu_1", "get_weather", @"{""city"":""Oslo""}"));

            await WithAnthropic(_ => Sse(sse), async (client, ctx) =>
            {
                using var streamed = new MemoryStream();
                var response = await client.StreamingCompleteAsync(ctx, client.Pool, AnswerPath,
                    new AiChatRequest { Messages = [Msg(ctx, "user", "weather?")], Schema = null },
                    m => { streamed.Write(m.Span); return Task.CompletedTask; },
                    new AiUsage(), trace: null, CancellationToken.None);

                Assert.Equal(AiResponseType.Tool, response.Type);
                Assert.Single(response.ToolCalls);
                Assert.Equal("get_weather", response.ToolCalls[0].Name);

                Assert.Equal("let me check that", Encoding.UTF8.GetString(streamed.ToArray()));
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Anthropic_ThinkingDeltas_NeverReachTheCallback_InEitherMode()
        {
            foreach (var schema in new[] { ChatCompletionClient.EmptySchema, null })
            {
                var structured = schema != null;
                var answer = structured ? @"{""Answer"":""visible""}" : "visible";
                var sse = AnthropicSse(textBlocks: [answer], thinking: "secret reasoning the user must not see");

                await WithAnthropic(_ => Sse(sse), async (client, ctx) =>
                {
                    using var streamed = new MemoryStream();
                    var response = await client.StreamingCompleteAsync(ctx, client.Pool, AnswerPath,
                        new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = schema },
                        m => { streamed.Write(m.Span); return Task.CompletedTask; },
                        new AiUsage(), trace: null, CancellationToken.None);

                    var streamedText = Encoding.UTF8.GetString(streamed.ToArray());
                    Assert.DoesNotContain("secret reasoning", streamedText);
                    Assert.Equal("visible", streamedText);

                    if (structured)
                    {
                        var obj = Assert.IsAssignableFrom<BlittableJsonReaderObject>(response.Result);
                        Assert.True(obj.TryGet(AnswerPath, out string parsed));
                        Assert.Equal("visible", parsed);
                    }
                    else
                    {
                        Assert.Equal("visible", Assert.IsType<string>(response.Result));
                        Assert.DoesNotContain("secret reasoning", (string)response.Result);
                    }
                });
            }
        }

        // ---- wire fixtures --------------------------------------------------------------------------------------

        private static string AnthropicSse(string[] textBlocks, string thinking = null, (string Id, string Name, string Input)? toolUse = null)
        {
            var sb = new StringBuilder();
            var index = 0;

            sb.Append(Event("message_start", new DynamicJsonValue
            {
                ["type"] = "message_start",
                ["message"] = new DynamicJsonValue
                {
                    ["role"] = "assistant",
                    ["usage"] = new DynamicJsonValue { ["input_tokens"] = 3, ["output_tokens"] = 2 }
                }
            }));

            if (thinking != null)
            {
                sb.Append(StartBlock(index, new DynamicJsonValue { ["type"] = "thinking", ["thinking"] = string.Empty }));
                sb.Append(Delta(index, new DynamicJsonValue { ["type"] = "thinking_delta", ["thinking"] = thinking }));
                sb.Append(Delta(index, new DynamicJsonValue { ["type"] = "signature_delta", ["signature"] = "sig-abc" }));
                sb.Append(StopBlock(index++));
            }

            foreach (var text in textBlocks)
            {
                sb.Append(StartBlock(index, new DynamicJsonValue { ["type"] = "text", ["text"] = string.Empty }));
                sb.Append(Delta(index, new DynamicJsonValue { ["type"] = "text_delta", ["text"] = text }));
                sb.Append(StopBlock(index++));
            }

            if (toolUse.HasValue)
            {
                var (id, name, input) = toolUse.Value;
                sb.Append(StartBlock(index, new DynamicJsonValue { ["type"] = "tool_use", ["id"] = id, ["name"] = name, ["input"] = new DynamicJsonValue() }));
                sb.Append(Delta(index, new DynamicJsonValue { ["type"] = "input_json_delta", ["partial_json"] = input }));
                sb.Append(StopBlock(index++));
            }

            sb.Append(Event("message_delta", new DynamicJsonValue
            {
                ["type"] = "message_delta",
                ["delta"] = new DynamicJsonValue { ["stop_reason"] = toolUse.HasValue ? "tool_use" : "end_turn" },
                ["usage"] = new DynamicJsonValue { ["output_tokens"] = 2 }
            }));
            sb.Append(Event("message_stop", new DynamicJsonValue { ["type"] = "message_stop" }));

            return sb.ToString();
        }

        private static string StartBlock(int i, DynamicJsonValue block) => Event("content_block_start", new DynamicJsonValue
        {
            ["type"] = "content_block_start", ["index"] = i, ["content_block"] = block
        });

        private static string Delta(int i, DynamicJsonValue delta) => Event("content_block_delta", new DynamicJsonValue
        {
            ["type"] = "content_block_delta", ["index"] = i, ["delta"] = delta
        });

        private static string StopBlock(int i) => Event("content_block_stop", new DynamicJsonValue
        {
            ["type"] = "content_block_stop", ["index"] = i
        });

        private static string Event(string name, DynamicJsonValue payload) => $"event: {name}\ndata: {Json(payload)}\n\n";

        private static string Json(DynamicJsonValue djv)
        {
            using var ctx = JsonOperationContext.ShortTermSingleUse();
            return ctx.ReadObject(djv, "fixture").ToString();
        }

        // ---- harness --------------------------------------------------------------------------------------------

        private static BlittableJsonReaderObject Msg(JsonOperationContext ctx, string role, string content) =>
            ctx.ReadObject(new DynamicJsonValue { ["role"] = role, ["content"] = content }, "msg");

        private static Task WithAnthropic(Func<string, HttpResponseMessage> respond, Func<CapturingChatCompletionClient, JsonOperationContext, Task> body) =>
            With(new AnthropicChatCompletionProvider(new AnthropicSettings("sk-ant-test", "claude-opus-4-8", "https://api.anthropic.com/v1/")), respond, body);

        private static async Task With(AbstractChatCompletionProvider settings, Func<string, HttpResponseMessage> respond,
            Func<CapturingChatCompletionClient, JsonOperationContext, Task> body)
        {
            using var storageEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests());
            using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), storageEnv);
            using var client = new CapturingChatCompletionClient(contextPool, settings, respond);
            using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
                await body(client, ctx);
        }
    }
}
