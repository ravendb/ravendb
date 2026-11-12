using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using SlowTests.Server.Documents.AI.AiAgent;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using static SlowTests.Server.Documents.AI.CapturingChatCompletionClient;

namespace SlowTests.Server.Documents.AI
{
    public class OpenAiResponsesProviderTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        // Conversations are stored in the Chat Completions shape, including ones stored before Responses existed;
        // every stored message kind must still reach the model correctly.
        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_FromFullHistory_TranslatesEveryStoredMessage()
        {
            var settings = Settings();
            settings.Seed = 42;

            await WithClient(settings, _ => Ok(ResponsesWire.Response("completed", ResponsesWire.TextMessage("ok"))), async (client, ctx) =>
            {
                var artificialId = Guid.NewGuid().ToString("N");
                var messages = new List<BlittableJsonReaderObject>
                {
                    Msg(ctx, "system", "You are helpful."),
                    Msg(ctx, "user", "hi"),
                    ctx.ReadObject(new DynamicJsonValue { ["role"] = "assistant", ["content"] = "hello", ["date"] = "2026-11-10", ["usage"] = new DynamicJsonValue { ["PromptTokens"] = 1 } }, "msg"),
                    ctx.ReadObject(new DynamicJsonValue { ["role"] = "assistant", ["content"] = new DynamicJsonValue { ["Answer"] = "x" } }, "msg"),
                    Msg(ctx, "assistant", ""),
                    ToolCallsMessage(ctx, "call_1", "GetOrder", "{\"Id\":1}"),
                    Msg(ctx, "tool", "{\"Total\":5}", toolCallId: "call_1"),
                    ctx.ReadObject(new DynamicJsonValue
                    {
                        // Chat Completions (non-streaming) stores the text and the tool calls in one message
                        ["role"] = "assistant",
                        ["content"] = "Checking order 2",
                        ["tool_calls"] = new DynamicJsonArray { ToolCall("call_2", "GetOrder", "{\"Id\":2}") }
                    }, "msg"),
                    Msg(ctx, "tool", "{\"Total\":7}", toolCallId: "call_2"),
                    ctx.ReadObject(new DynamicJsonValue
                    {
                        // ConversationDocument.AddArtificialToolCall: GUID "N" id, no content field
                        ["role"] = "assistant",
                        ["tool_calls"] = new DynamicJsonArray { ToolCall(artificialId, "RecentOrders", "{}") }
                    }, "msg"),
                    Msg(ctx, "tool", "[]", toolCallId: artificialId),
                    Msg(ctx, "system", "Second rule."),
                    ctx.ReadObject(new DynamicJsonValue { ["role"] = "user", ["content"] = "summarize", ["max_completion_tokens"] = 100 }, "msg")
                };

                var request = new AiChatRequest
                {
                    Messages = messages,
                    Attachments =
                    [
                        new AiAttachment { Name = "notes.txt", Type = "text/plain", Data = "some notes" },
                        new AiAttachment { Name = "pic.png", Type = "image/png", Data = "iVBORw0K" },
                        new AiAttachment { Name = "doc.pdf", Type = "application/pdf", Data = "JVBERi0x" },
                        new AiAttachment { Name = "gone.png", Type = "image/png", Source = AiAttachmentSource.NotFound }
                    ]
                };

                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

                var body = JObject.Parse(client.LastRequestBody);
                Assert.Equal("gpt-5.4", (string)body["model"]);
                Assert.Equal("You are helpful.\n\nSecond rule.", (string)body["instructions"]);
                Assert.False((bool)body["store"]);
                Assert.Null(body["seed"]);
                Assert.Null(body["messages"]);
                Assert.Null(body["response_format"]);
                Assert.Null(body["previous_response_id"]);
                Assert.Null(body["stream"]);
                Assert.Null(body["text"]);

                var input = (JArray)body["input"];
                var expected = new JArray(
                    JObject.Parse("""{"role":"user","content":"hi"}"""),
                    JObject.Parse("""{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}"""),
                    JObject.Parse("""{"type":"message","role":"assistant","content":[{"type":"output_text","text":"{\"Answer\":\"x\"}"}]}"""),
                    // the empty assistant turn is skipped
                    JObject.Parse("""{"type":"function_call","call_id":"call_1","name":"GetOrder","arguments":"{\"Id\":1}"}"""),
                    JObject.Parse("""{"type":"function_call_output","call_id":"call_1","output":"{\"Total\":5}"}"""),
                    // the text first, then the call
                    JObject.Parse("""{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Checking order 2"}]}"""),
                    JObject.Parse("""{"type":"function_call","call_id":"call_2","name":"GetOrder","arguments":"{\"Id\":2}"}"""),
                    JObject.Parse("""{"type":"function_call_output","call_id":"call_2","output":"{\"Total\":7}"}"""),
                    JObject.Parse($$"""{"type":"function_call","call_id":"{{artificialId}}","name":"RecentOrders","arguments":"{}"}"""),
                    JObject.Parse($$"""{"type":"function_call_output","call_id":"{{artificialId}}","output":"[]"}"""),
                    // "max_completion_tokens" stored inside the message is dropped
                    JObject.Parse("""{"role":"user","content":"summarize"}"""),
                    JObject.Parse("""
                        {"role":"user","content":[
                            {"type":"input_text","text":"some notes"},
                            {"type":"input_image","image_url":"data:image/png;base64,iVBORw0K"},
                            {"type":"input_file","filename":"doc.pdf","file_data":"data:application/pdf;base64,JVBERi0x"},
                            {"type":"input_text","text":"File 'gone.png' (of type 'image/png') could not be loaded: attachment not found"}
                        ]}
                        """));

                Assert.True(JToken.DeepEquals(expected, input), $"Expected:{Environment.NewLine}{expected}{Environment.NewLine}Actual:{Environment.NewLine}{input}");
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Refusal_Throws(bool streaming)
        {
            var body = streaming
                ? ResponsesWire.Stream(ResponsesWire.RefusalDelta("I can't "), ResponsesWire.RefusalDelta("help."), ResponsesWire.Completed())
                : ResponsesWire.Response("completed", ResponsesWire.RefusalMessage("I can't help."));

            await WithClient(Settings(), _ => streaming ? Sse(body) : Ok(body), async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAsync<RefusedToAnswerException>(() => SendAsync(client, ctx, streaming, structured: true));
                Assert.Equal("I can't help.", ex.Refusal);
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task IncompleteContentFilter_Throws(bool streaming)
        {
            var body = streaming
                ? ResponsesWire.Stream(ResponsesWire.TextDelta("partial"), ResponsesWire.Incomplete("content_filter"))
                : ResponsesWire.IncompleteResponse("content_filter", ResponsesWire.TextMessage("partial"));

            await WithClient(Settings(), _ => streaming ? Sse(body) : Ok(body), async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAsync<RefusedToAnswerException>(() => SendAsync(client, ctx, streaming, structured: false));
                Assert.Equal("content_filter", ex.FinishReason);
            });
        }

        // a cut plain-text answer is still the answer; a cut JSON answer, tool call or empty answer is too many tokens
        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(false, "plain")]
        [InlineData(false, "structured")]
        [InlineData(false, "tool")]
        [InlineData(false, "plain-empty")]
        [InlineData(true, "plain")]
        [InlineData(true, "structured")]
        public async Task IncompleteMaxOutputTokens(bool streaming, string kind)
        {
            var body = streaming
                ? ResponsesWire.Stream(ResponsesWire.TextDelta(kind == "structured" ? "{\"Answer\":\"cut" : "The answer is"), ResponsesWire.Incomplete("max_output_tokens"))
                : ResponsesWire.IncompleteResponse("max_output_tokens", kind switch
                {
                    "structured" => ResponsesWire.TextMessage("{\"Answer\":\"cut"),
                    "tool" => ResponsesWire.FunctionCall("call_1", "GetOrder", "{\"Id\":"),
                    "plain-empty" => ResponsesWire.Reasoning("thinking..."),
                    _ => ResponsesWire.TextMessage("The answer is")
                });

            await WithClient(Settings(), _ => streaming ? Sse(body) : Ok(body), async (client, ctx) =>
            {
                var usage = new AiUsage();
                if (kind == "plain")
                {
                    var response = await SendAsync(client, ctx, streaming, structured: false, usage);
                    Assert.Equal("The answer is", response.Result);
                    return;
                }

                await Assert.ThrowsAsync<TooManyTokensException>(() => SendAsync(client, ctx, streaming, structured: kind == "structured", usage));
                Assert.Equal(ResponsesWire.InputTokens, usage.PromptTokens); // a cut still uses tokens
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Failed_Throws_AndCountsUsage(bool streaming)
        {
            var body = streaming
                ? ResponsesWire.Stream(ResponsesWire.TextDelta("He"), ResponsesWire.Failed("server_error", "The model crashed"))
                : ResponsesWire.FailedResponse("server_error", "The model crashed");

            await WithClient(Settings(), _ => streaming ? Sse(body) : Ok(body), async (client, ctx) =>
            {
                var usage = new AiUsage();
                var ex = await Assert.ThrowsAsync<UnexpectedResponseException>(() => SendAsync(client, ctx, streaming, structured: false, usage));

                Assert.Contains("The model crashed", ex.Message);
                Assert.Contains("server_error", ex.Message);
                AssertUsage(usage);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_TwoCallsWithInterleavedArgumentDeltas()
        {
            var sse = ResponsesWire.Stream(
                ResponsesWire.FunctionCallAdded("fc_1", "call_1", "GetOrder"),
                ResponsesWire.FunctionCallAdded("fc_2", "call_2", "GetCustomer"),
                ResponsesWire.ArgumentsDelta("fc_1", "{\"Id\":"),
                ResponsesWire.ArgumentsDelta("fc_2", "{\"Name\":"),
                ResponsesWire.ArgumentsDelta("fc_1", "\"orders/1\"}"),
                ResponsesWire.ArgumentsDelta("fc_2", "\"Jane\"}"),
                ResponsesWire.FunctionCallDone("fc_1", "call_1", "GetOrder", "{\"Id\":\"orders/1\"}"),
                ResponsesWire.FunctionCallDone("fc_2", "call_2", "GetCustomer", "{\"Name\":\"Jane\"}"),
                ResponsesWire.Completed());

            await WithClient(Settings(), _ => Sse(sse), async (client, ctx) =>
            {
                var streamed = new MemoryStream();
                var response = await client.StreamingCompleteAsync(ctx, client.Pool, "Answer", new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = ChatCompletionClient.EmptySchema },
                    m => { streamed.Write(m.Span); return Task.CompletedTask; }, new AiUsage(), null, CancellationToken.None);

                Assert.Equal(AiResponseType.Tool, response.Type);
                Assert.Equal(new[] { new AiToolCall("call_1", "GetOrder", "{\"Id\":\"orders/1\"}"), new AiToolCall("call_2", "GetCustomer", "{\"Name\":\"Jane\"}") }, response.ToolCalls);
                Assert.Empty(streamed.ToArray());
                AssertJson("""
                    {"role":"assistant","content":null,"tool_calls":[
                        {"id":"call_1","type":"function","function":{"name":"GetOrder","arguments":"{\"Id\":\"orders/1\"}"}},
                        {"id":"call_2","type":"function","function":{"name":"GetCustomer","arguments":"{\"Name\":\"Jane\"}"}}]}
                    """, response.Message);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_ErrorEvent_Throws()
        {
            var sse = ResponsesWire.Stream(ResponsesWire.TextDelta("He"), ResponsesWire.StreamError("rate_limit_exceeded", "Slow down"));

            await WithClient(Settings(), _ => Sse(sse), async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAsync<UnexpectedResponseException>(() => SendAsync(client, ctx, streaming: true, structured: false));
                Assert.Contains("Slow down", ex.Message);
                Assert.Contains("rate_limit_exceeded", ex.Message);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_CutBeforeCompleted_Throws()
        {
            var sse = ResponsesWire.Stream(ResponsesWire.TextDelta("Hel"), ResponsesWire.TextDelta("lo"));

            await WithClient(Settings(), _ => Sse(sse), async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAsync<UnexpectedResponseException>(() => SendAsync(client, ctx, streaming: true, structured: false));
                Assert.Contains("stream ended before", ex.Message);
            });
        }

        // T9
        [RavenFact(RavenTestCategory.Ai)]
        public async Task Agent_ToolRoundTrip_StoresChatCompletionsShape()
        {
            using var store = GetDocumentStore();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new { Company = "companies/1-A", Product = "a widget" }, "orders/1-A");
                await session.SaveChangesAsync();
            }

            var agent = new AiAgentConfiguration("shopping assistant", "fake-connection", "You answer shop questions.")
            {
                Queries = [new AiAgentToolQuery("RecentOrder", "Get the recent orders", "from @all_docs where Company = 'companies/1-A'") { ParametersSampleObject = "{}" }],
                SampleObject = "{\"Answer\":\"The answer to the query\"}"
            };

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            const string conversationId = "chats/responses-t9";
            var payloads = new List<JObject>();

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: payload =>
                    {
                        payloads.Add(payload);
                        var body = payloads.Count == 1
                            ? ResponsesWire.Response("completed", ResponsesWire.FunctionCall("call_t9", "RecentOrder", "{}"))
                            : ResponsesWire.Response("completed", ResponsesWire.TextMessage("{\"Answer\":\"a widget\"}"));
                        return Ok(body);
                    },
                    clientSettings: new OpenAiResponsesProvider(Settings()))
                {
                    Authentication = null
                };

                handler.Initialize(agent, conversationId, new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "what did I order"
                }, changeVector: null);

                var result = await handler.HandleRequestAsync(context, CancellationToken.None);
                Assert.Contains("a widget", result.Response.ToString());
            }

            Assert.Equal(2, payloads.Count);
            var secondInput = (JArray)payloads[1]["input"];
            Assert.Contains(secondInput, i => (string)i["type"] == "function_call" && (string)i["call_id"] == "call_t9" && (string)i["name"] == "RecentOrder");
            Assert.Contains(secondInput, i => (string)i["type"] == "function_call_output" && (string)i["call_id"] == "call_t9" && ((string)i["output"]).Contains("a widget"));
            Assert.Contains("You answer shop questions.", (string)payloads[0]["instructions"]);
            Assert.DoesNotContain((JArray)payloads[0]["input"], i => (string)i["role"] == "system");

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = database.DocumentsStorage.Get(context, conversationId);
                Assert.NotNull(document);
                var messages = (JArray)JObject.Parse(document.Data.ToString())["Messages"];

                var toolCallMessage = messages.Single(m => m["tool_calls"] != null);
                Assert.Equal("assistant", (string)toolCallMessage["role"]);
                var storedCall = (JObject)toolCallMessage["tool_calls"]!.Single();
                Assert.Equal("call_t9", (string)storedCall["id"]);
                Assert.Equal("function", (string)storedCall["type"]);
                Assert.Equal("RecentOrder", (string)storedCall["function"]!["name"]);

                var toolMessage = messages.Single(m => (string)m["role"] == "tool");
                Assert.Equal("call_t9", (string)toolMessage["tool_call_id"]);

                var answer = messages.Last(m => (string)m["role"] == "assistant");
                Assert.Equal("a widget", (string)answer["content"]!["Answer"]);

                // nothing Responses-shaped is persisted
                var all = messages.ToString();
                Assert.DoesNotContain("\"call_id\"", all);
                Assert.DoesNotContain("function_call", all);
                Assert.DoesNotContain("output_text", all);
            }
        }

        // T10
        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(null, null, true)]
        [InlineData("", null, true)]
        [InlineData("https://api.openai.com/", null, true)]
        [InlineData("https://api.openai.com/v1", null, true)]
        [InlineData("https://API.OpenAI.com/v1/", null, true)]
        // existing connection strings to other servers keep Chat Completions
        [InlineData("https://my.host/v1", null, false)]
        [InlineData("https://api.openai.com.my.host/v1", null, false)]
        [InlineData(null, OpenAiApiMode.ChatCompletions, false)]
        [InlineData("https://my.host/v1", OpenAiApiMode.ChatCompletions, false)]
        [InlineData("https://my.host/v1", OpenAiApiMode.Responses, true)]
        public void TryGetParameters_PicksTheApi(string endpoint, OpenAiApiMode? apiMode, bool expectResponses)
        {
            var connection = new AiConnectionString
            {
                Name = "openai",
                ModelType = AiModelType.Chat,
                OpenAiSettings = new OpenAiSettings("sk-test", endpoint, "gpt-5.4") { ApiMode = apiMode }
            };

            Assert.True(AbstractChatCompletionProvider.TryGetParameters(connection, out var provider));

            if (expectResponses)
                Assert.IsType<OpenAiResponsesProvider>(provider);
            else
                Assert.IsType<OpenAiChatCompletionProvider>(provider);

            // the address the request goes to, as ChatCompletionClient builds it (base address + relative completion URI)
            var requestPath = new Uri(provider.GetBaseEndpointUri(), provider.GetRelativeCompletionUri()).AbsolutePath;
            var expectedEnd = expectResponses ? "/v1/responses" : "/v1/chat/completions";
            Assert.EndsWith(expectedEnd, requestPath, StringComparison.OrdinalIgnoreCase);
        }

        private static OpenAiSettings Settings() => new("sk-test", null, "gpt-5.4");

        private static async Task WithClient(OpenAiSettings settings, Func<string, HttpResponseMessage> respond, Func<CapturingChatCompletionClient, JsonOperationContext, Task> body)
        {
            using var contextPool = new JsonContextPool();
            using var client = new CapturingChatCompletionClient(contextPool, new OpenAiResponsesProvider(settings), respond);
            using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
                await body(client, ctx);
        }

        private static Task<AiResponse> SendAsync(CapturingChatCompletionClient client, JsonOperationContext ctx, bool streaming, bool structured, AiUsage usage = null)
        {
            var request = new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = structured ? ChatCompletionClient.EmptySchema : null };
            usage ??= new AiUsage();

            return streaming
                ? client.StreamingCompleteAsync(ctx, client.Pool, "Answer", request, _ => Task.CompletedTask, usage, null, CancellationToken.None)
                : client.CompleteAsync(ctx, request, usage, null, CancellationToken.None);
        }

        private static BlittableJsonReaderObject Msg(JsonOperationContext ctx, string role, string content, string toolCallId = null)
        {
            var json = new DynamicJsonValue { ["role"] = role, ["content"] = content };
            if (toolCallId != null)
                json["tool_call_id"] = toolCallId;
            return ctx.ReadObject(json, "msg");
        }

        private static DynamicJsonValue ToolCall(string id, string name, string arguments) => new()
        {
            ["id"] = id,
            ["type"] = "function",
            ["function"] = new DynamicJsonValue { ["name"] = name, ["arguments"] = arguments }
        };

        private static BlittableJsonReaderObject ToolCallsMessage(JsonOperationContext ctx, string id, string name, string arguments) =>
            ctx.ReadObject(new DynamicJsonValue { ["role"] = "assistant", ["content"] = null, ["tool_calls"] = new DynamicJsonArray { ToolCall(id, name, arguments) } }, "msg");

        private static void AssertJson(string expected, BlittableJsonReaderObject actual)
        {
            var expectedJson = JObject.Parse(expected);
            var actualJson = JObject.Parse(actual.ToString());
            Assert.True(JToken.DeepEquals(expectedJson, actualJson), $"Expected:{Environment.NewLine}{expectedJson}{Environment.NewLine}Actual:{Environment.NewLine}{actualJson}");
        }

        private static void AssertUsage(AiUsage usage)
        {
            Assert.Equal(ResponsesWire.InputTokens, usage.PromptTokens);
            Assert.Equal(ResponsesWire.OutputTokens, usage.CompletionTokens);
            Assert.Equal(ResponsesWire.TotalTokens, usage.TotalTokens);
            Assert.Equal(ResponsesWire.CachedTokens, usage.CachedTokens);
            Assert.Equal(ResponsesWire.ReasoningTokens, usage.ReasoningTokens);
        }
    }
}
