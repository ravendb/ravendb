using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString.AI;
using Voron;
using Xunit;
using static SlowTests.Server.Documents.AI.CapturingChatCompletionClient;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AnthropicChatCompletionClientTests : RavenTestBase
    {
        public AnthropicChatCompletionClientTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string TextResponse =
            """
            {"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"{\"Answer\":\"yes\"}"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":2,"output_tokens_details":{"thinking_tokens":3}}}
            """;

        private const string ToolUseWithThinkingResponse =
            """
            {"id":"msg_2","type":"message","role":"assistant","content":[{"type":"thinking","thinking":"let me think","signature":"sig123"},{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Paris"}}],"stop_reason":"tool_use","usage":{"input_tokens":20,"output_tokens":8}}
            """;

        // ---- provider routing (the one split point both Agents and GenAI ETL go through) ------------------------

        [RavenFact(RavenTestCategory.Ai)]
        public void Factory_RoutesAnthropicConnection_ToNativeClient_ElseToOpenAiFamily()
        {
            using var storageEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests());
            using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), storageEnv);

            var anthropic = new AiConnectionString
            {
                Name = "claude",
                ModelType = AiModelType.Chat,
                AnthropicSettings = new AnthropicSettings("sk-ant-test", "claude-opus-4-8", "https://api.anthropic.com/v1/")
            };
            using (var client = ChatCompletionClient.CreateChatCompletionClient(contextPool, anthropic))
                Assert.IsType<AnthropicChatCompletionProvider>(client.ForTestingPurposesOnly().Provider);

            var ollama = new AiConnectionString
            {
                Name = "ollama",
                ModelType = AiModelType.Chat,
                OllamaSettings = new OllamaSettings { Uri = "http://localhost:11434", Model = "x" }
            };
            using (var client = ChatCompletionClient.CreateChatCompletionClient(contextPool, ollama))
            {
                Assert.IsType<ChatCompletionClient>(client);
                Assert.IsNotType<AnthropicChatCompletionProvider>(client.ForTestingPurposesOnly().Provider);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public void TestConnection_DeserializesAnthropicSettings_AndResolvesTheProvider()
        {
            using var storageEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests());
            using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), storageEnv);
            using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                var json = ctx.ReadObject(new DynamicJsonValue
                {
                    ["ApiKey"] = "sk-ant-test",
                    ["Model"] = "claude-sonnet-4-5",
                    ["Endpoint"] = "https://api.anthropic.com/v1/",
                    ["MaxOutputTokens"] = 4096
                }, "anthropic/settings");

                var settings = JsonDeserializationServer.AnthropicSettings(json);
                Assert.Equal("sk-ant-test", settings.ApiKey);
                Assert.Equal("claude-sonnet-4-5", settings.Model);
                Assert.Equal(4096, settings.MaxOutputTokens);

                var connectionString = new AiConnectionString { Name = "claude", ModelType = AiModelType.Chat, AnthropicSettings = settings };
                Assert.Equal(AiConnectorType.Anthropic, connectionString.GetActiveProvider());
            }
        }

        // ---- request translation ---------------------------------------------------------------------------------

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_HoistsSystem_TranslatesUser_UnwrapsSchema_SetsMaxTokens()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var request = new AiChatRequest
                {
                    Messages = [Msg(ctx, "system", "You are helpful."), Msg(ctx, "user", "Hi")],
                    Schema = ChatCompletionClient.GetSchemaFromSampleObject("{\"Answer\":\"the answer\"}")
                };

                var usage = new AiUsage();
                var response = await client.CompleteAsync(ctx, request, usage, trace: null, CancellationToken.None);

                var body = JObject.Parse(client.LastRequestBody);
                Assert.Equal("You are helpful.", (string)body["system"]);
                Assert.Equal("claude-opus-4-8", (string)body["model"]);
                Assert.Equal(16000, (int)body["max_tokens"]);
                Assert.Equal("user", (string)body["messages"][0]["role"]);
                Assert.Equal("text", (string)body["messages"][0]["content"][0]["type"]);
                Assert.Equal("Hi", (string)body["messages"][0]["content"][0]["text"]);
                Assert.Equal("json_schema", (string)body["output_config"]["format"]["type"]);
                Assert.Equal("object", (string)body["output_config"]["format"]["schema"]["type"]); // unwrapped inner schema

                Assert.Equal(AiResponseType.Result, response.Type);
                Assert.True(((BlittableJsonReaderObject)response.Result).TryGet("Answer", out string answer));
                Assert.Equal("yes", answer);
                Assert.Equal(12, usage.PromptTokens);   // input(10) + cache_read(2)
                Assert.Equal(5, usage.CompletionTokens);
                Assert.Equal(2, usage.CachedTokens);
                Assert.Equal(3, usage.ReasoningTokens);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_UsesXApiKeyAndAnthropicVersionHeaders()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                await client.CompleteAsync(ctx, new AiChatRequest { Messages = [Msg(ctx, "user", "Hi")], Schema = EmptySchema() }, new AiUsage(), null, CancellationToken.None);

                Assert.Equal("sk-ant-test", client.LastHeader("x-api-key"));
                Assert.Equal("2023-06-01", client.LastHeader("anthropic-version"));
                Assert.Null(client.LastHeader("Authorization")); // native uses x-api-key, never Bearer
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_UsesConfiguredApiVersion()
        {
            var settings = new AnthropicSettings("sk-ant-test", "claude-opus-5", "https://api.anthropic.com/v1/") { ApiVersion = "2099-01-01" };
            await WithClient(settings, _ => Ok(TextResponse), async (client, ctx) =>
            {
                await client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None);

                Assert.Equal("2099-01-01", client.LastHeader("anthropic-version"));
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task GenAi_PromptCacheKey_OffForAnthropic_UnchangedForOtherProviders()
        {
            using var store = GetDocumentStore();
            var database = await GetDatabase(store.Database);

            var anthropic = new GenAiConfiguration { Connection = new AiConnectionString { AnthropicSettings = new AnthropicSettings("k", "claude-haiku-4-5") } };
            var openAi = new GenAiConfiguration { Connection = new AiConnectionString { OpenAiSettings = new OpenAiSettings("k", "https://api.openai.com/v1/", "gpt-4o-mini") } };

            Assert.Null(new GenAiConversationHandler(Server.ServerStore, database, anthropic) { Authentication = null }.GetPromptCacheKey("genai/doc/1"));
            Assert.Equal("genai/doc/1", new GenAiConversationHandler(Server.ServerStore, database, openAi) { Authentication = null }.GetPromptCacheKey("genai/doc/1"));
            Assert.Equal("chats/1", new ConversationHandler(Server.ServerStore, database) { Authentication = null }.GetPromptCacheKey("chats/1"));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task ToolDescriptors_BuiltForTurnAndSummary_RetrieveAttachmentOnce()
        {
            // Anthropic rejects duplicate tool names (400 "tools: Tool names must be unique"), and the summary request
            // builds the tools again from the same agent configuration.
            using var store = GetDocumentStore();
            var database = await GetDatabase(store.Database);
            var handler = new ConversationHandler(Server.ServerStore, database) { Authentication = null, _persistedAttachmentsNames = ["photo.png"] };
            var configuration = new AiAgentConfiguration
            {
                Actions = [new AiAgentToolAction { Name = "do_thing", Description = "Does a thing", ParametersSampleObject = "{\"x\":\"y\"}" }]
            };

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                var turn = handler.BuildToolDescriptors(ctx, configuration);
                var summary = handler.BuildToolDescriptors(ctx, configuration);

                Assert.Single(turn, t => t.Name == ChatCompletionClient.Constants.ToolNames.RetrieveAttachment);
                Assert.Single(summary, t => t.Name == ChatCompletionClient.Constants.ToolNames.RetrieveAttachment);
                Assert.Equal(["do_thing", ChatCompletionClient.Constants.ToolNames.RetrieveAttachment], summary.Select(t => t.Name));
                Assert.Single(configuration.Actions, a => a.Name == ChatCompletionClient.Constants.ToolNames.RetrieveAttachment);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_TranslatesToolsWithStrict_AndToolChoiceNoneWhenNotUsed()
        {
            const string toolSchema = "{\"type\":\"object\",\"properties\":{\"city\":{\"type\":\"string\"}},\"required\":[\"city\"],\"additionalProperties\":false}";
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var request = new AiChatRequest
                {
                    Messages = [Msg(ctx, "user", "Hi")],
                    Tools = client.PrepareTools(ctx, [new AiToolDescriptor("get_weather", "Get the weather", toolSchema)]),
                    UseTools = false,
                    Schema = EmptySchema()
                };

                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

                var body = JObject.Parse(client.LastRequestBody);
                Assert.Equal("get_weather", (string)body["tools"][0]["name"]);
                Assert.Equal("Get the weather", (string)body["tools"][0]["description"]);
                Assert.Equal(true, (bool)body["tools"][0]["strict"]);
                Assert.Equal("object", (string)body["tools"][0]["input_schema"]["type"]);
                Assert.Equal("none", (string)body["tool_choice"]["type"]); // UseTools == false
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_GroupsConsecutiveToolResultsIntoOneUserTurn()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var assistant = ctx.ReadObject(new DynamicJsonValue
                {
                    ["role"] = "assistant",
                    ["content"] = null,
                    ["tool_calls"] = new DynamicJsonArray
                    {
                        ToolCall("call_1", "get_weather", "{}"),
                        ToolCall("call_2", "get_time", "{}")
                    }
                }, "assistant");

                var request = new AiChatRequest
                {
                    Messages =
                    [
                        Msg(ctx, "user", "Hi"),
                        assistant,
                        Msg(ctx, "tool", "sunny", toolCallId: "call_1"),
                        Msg(ctx, "tool", "noon", toolCallId: "call_2")
                    ],
                    Schema = EmptySchema()
                };

                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

                var messages = (JArray)JObject.Parse(client.LastRequestBody)["messages"];
                var toolResultTurn = messages.Last(m => (string)m["role"] == "user" &&
                                                        m["content"] is JArray c && c.Count > 0 && (string)c[0]["type"] == "tool_result");
                var blocks = (JArray)toolResultTurn["content"];
                Assert.Equal(2, blocks.Count);
                Assert.Equal("call_1", (string)blocks[0]["tool_use_id"]);
                Assert.Equal("sunny", (string)blocks[0]["content"]);
                Assert.Equal("call_2", (string)blocks[1]["tool_use_id"]);
                Assert.Equal("noon", (string)blocks[1]["content"]);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_ReplaysPriorStructuredAssistantAnswer_AsText_NotEmpty()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var priorAnswer = ctx.ReadObject(new DynamicJsonValue
                {
                    ["role"] = "assistant",
                    ["content"] = new DynamicJsonValue { ["Answer"] = "yes" }
                }, "assistant");

                var request = new AiChatRequest
                {
                    Messages = [Msg(ctx, "user", "first?"), priorAnswer, Msg(ctx, "user", "and now?")],
                    Schema = EmptySchema()
                };

                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

                var messages = (JArray)JObject.Parse(client.LastRequestBody)["messages"];
                var assistantContent = (JArray)messages.First(m => (string)m["role"] == "assistant")["content"];
                Assert.NotEmpty(assistantContent);
                Assert.Equal("text", (string)assistantContent[0]["type"]);
                Assert.Contains("Answer", (string)assistantContent[0]["text"]); // the structured answer, stringified
                Assert.Contains("yes", (string)assistantContent[0]["text"]);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_MultiPartUserContent_BecomesSeparateTextBlocks_NotRawJson()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var multiPart = ctx.ReadObject(new DynamicJsonValue
                {
                    ["role"] = "user",
                    ["content"] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { ["type"] = "text", ["text"] = "part one" },
                        new DynamicJsonValue { ["type"] = "text", ["text"] = "part two" }
                    }
                }, "user");

                await client.CompleteAsync(ctx, new AiChatRequest { Messages = [multiPart], Schema = EmptySchema() }, new AiUsage(), null, CancellationToken.None);

                var messages = (JArray)JObject.Parse(client.LastRequestBody)["messages"];
                var content = (JArray)messages.First(m => (string)m["role"] == "user")["content"];
                Assert.Equal(2, content.Count);
                Assert.Equal("part one", (string)content[0]["text"]);
                Assert.Equal("part two", (string)content[1]["text"]);
            });
        }

        // ---- extended thinking (request side) --------------------------------------------------------------------

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_MergesEffortAndFormat_IntoOneOutputConfig()
        {
            var settings = new AnthropicSettings("sk-ant-test", "claude-opus-5", "https://api.anthropic.com/v1/", maxOutputTokens: 512, reasoningEffort: "high");
            await WithClient(settings, _ => Ok(TextResponse), async (client, ctx) =>
            {
                await client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None);

                var body = JObject.Parse(client.LastRequestBody);
                Assert.Equal(512, (int)body["max_tokens"]); // the configured cap, not the default
                var outputConfig = (JObject)body["output_config"];
                Assert.Equal("high", (string)outputConfig["effort"]);
                Assert.Equal("json_schema", (string)outputConfig["format"]["type"]);
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData("some-unreleased-model", "max")]
        public async Task Request_Reasoning_SendsAdaptiveThinkingAndEffort(string model, string effort)
        {
            var settings = new AnthropicSettings("sk-ant-test", model, "https://api.anthropic.com/v1/", maxOutputTokens: 8192, reasoningEffort: effort);
            await WithClient(settings, _ => Ok(TextResponse), async (client, ctx) =>
            {
                await client.CompleteAsync(ctx, new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = null },
                    new AiUsage(), null, CancellationToken.None);

                var body = JObject.Parse(client.LastRequestBody);
                Assert.Equal("adaptive", (string)body["thinking"]["type"]);
                Assert.Equal(effort, (string)body["output_config"]["effort"]);
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(" HIGH ", "high")]
        [InlineData("XHigh", "xhigh")]
        public async Task Request_EffortLevel_IsTrimmedAndLowercased(string reasoning, string expected)
        {
            var settings = new AnthropicSettings("sk-ant-test", "claude-opus-5", "https://api.anthropic.com/v1/", maxOutputTokens: 8192, reasoningEffort: reasoning);
            await WithClient(settings, _ => Ok(TextResponse), async (client, ctx) =>
            {
                await client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None);

                Assert.Equal(expected, (string)JObject.Parse(client.LastRequestBody)["output_config"]["effort"]);
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData("claude-opus-5")]
        public async Task Request_OmitsThinking_ByDefault(string model)
        {
            var settings = new AnthropicSettings("sk-ant-test", model, "https://api.anthropic.com/v1/", maxOutputTokens: 8192);
            await WithClient(settings, _ => Ok(TextResponse), async (client, ctx) =>
            {
                await client.CompleteAsync(ctx, new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = null },
                    new AiUsage(), null, CancellationToken.None);

                var body = JObject.Parse(client.LastRequestBody);
                Assert.Null(body["thinking"]);
                Assert.Null(body["output_config"]?["effort"]);
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData("conversations/1", null, true)]
        [InlineData("conversations/1", true, true)]
        [InlineData("conversations/1", false, false)]
        [InlineData(null, null, false)]
        public async Task Request_PromptCaching_OnlyForRequestsWithACacheKey_UnlessDisabled(string promptCacheKey, bool? enablePromptCache, bool cached)
        {
            var settings = new AnthropicSettings("sk-ant-test", "claude-opus-5", "https://api.anthropic.com/v1/") { EnablePromptCache = enablePromptCache };
            await WithClient(settings, _ => Ok(TextResponse), async (client, ctx) =>
            {
                var request = Simple(ctx);
                request.PromptCacheKey = promptCacheKey;
                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

                var cacheControl = JObject.Parse(client.LastRequestBody)["cache_control"];
                if (cached)
                    Assert.Equal("ephemeral", (string)cacheControl["type"]);
                else
                    Assert.Null(cacheControl);
            });
        }

        // ---- empty canonical content (must never become an empty text block) --------------------------------------

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData("")]
        [InlineData("   ")]
        public async Task Request_EmptyUserContent_SkipsTheTurn_InsteadOfSendingAnEmptyTextBlock(string content)
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                await client.CompleteAsync(ctx, new AiChatRequest
                {
                    Messages = [Msg(ctx, "user", content), Msg(ctx, "user", "real question")],
                    Schema = EmptySchema()
                }, new AiUsage(), null, CancellationToken.None);

                var messages = (JArray)JObject.Parse(client.LastRequestBody)["messages"];
                Assert.Single(messages);
                Assert.Equal("real question", (string)messages[0]["content"][0]["text"]);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_MixedEmptyAndValidParts_KeepsOnlyTheValidOnes()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var mixed = ctx.ReadObject(new DynamicJsonValue
                {
                    ["role"] = "user",
                    ["content"] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { ["type"] = "text", ["text"] = "" },
                        new DynamicJsonValue { ["type"] = "text", ["text"] = "keep me" },
                        new DynamicJsonValue { ["type"] = "text", ["text"] = "" }
                    }
                }, "msg");

                await client.CompleteAsync(ctx, new AiChatRequest { Messages = [mixed], Schema = EmptySchema() },
                    new AiUsage(), null, CancellationToken.None);

                var blocks = (JArray)((JArray)JObject.Parse(client.LastRequestBody)["messages"])[0]["content"];
                Assert.Single(blocks);
                Assert.Equal("keep me", (string)blocks[0]["text"]);
            });
        }

        // ---- settings -------------------------------------------------------------------------------------------------

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(null, "https://api.anthropic.com/v1/")]
        [InlineData("https://api.anthropic.com", "https://api.anthropic.com/v1/")]
        [InlineData("https://api.anthropic.com/", "https://api.anthropic.com/v1/")]
        [InlineData("https://api.anthropic.com/v1", "https://api.anthropic.com/v1/")]
        [InlineData("https://proxy.example.com/anthropic/v1/", "https://proxy.example.com/anthropic/v1/")]
        public void Endpoint_TheDocumentedBaseUrlGetsTheVersionSegment(string endpoint, string expected)
        {
            Assert.Equal(expected, new AnthropicSettings("k", "m", endpoint: endpoint).GetBaseEndpointUri().ToString());
        }

        [RavenFact(RavenTestCategory.Ai)]
        public void EnablePromptCache_ChangeIsAChange_AndIsSerialized()
        {
            var before = new AnthropicSettings("k", "m");
            var after = new AnthropicSettings("k", "m") { EnablePromptCache = false };

            Assert.NotEqual(AiSettingsCompareDifferences.None, before.Compare(after));
            Assert.Equal(false, after.ToJson()[nameof(AnthropicSettings.EnablePromptCache)]);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public void ApiVersion_ChangeIsAChange_AndIsSerializedOnlyWhenSet()
        {
            var before = new AnthropicSettings("k", "m");
            var after = new AnthropicSettings("k", "m") { ApiVersion = "2099-01-01" };

            Assert.NotEqual(AiSettingsCompareDifferences.None, before.Compare(after));
            Assert.Equal("2099-01-01", after.ToJson()[nameof(AnthropicSettings.ApiVersion)]);
            Assert.DoesNotContain(before.ToJson().Properties, x => x.Name == nameof(AnthropicSettings.ApiVersion));
        }

        // ---- empty conversations and summaries ------------------------------------------------------------------

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(false)]
        public async Task EveryMessageEmpty_Throws(bool streaming)
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var request = new AiChatRequest { Messages = [Msg(ctx, "user", ""), Msg(ctx, "user", "   ")], Schema = EmptySchema() };

                // Normalization runs while the body is written, so a real HttpClient wraps this in an
                // HttpRequestException while this handler does not. The explanation is in the chain either way.
                var ex = await Assert.ThrowsAnyAsync<Exception>(() => RunAsync(client, ctx, request, streaming));

                Assert.Contains("every message was empty", ex.ToString());
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task PostSummarization_LeadingSummary_StaysAnAssistantTurn()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var summary = ctx.ReadObject(new DynamicJsonValue
                {
                    ["role"] = "assistant",
                    ["content"] = "Summary: the user asked about fruit and was told apples are red.",
                    [ConversationDocument.SummaryProperty] = true
                }, "summary-msg");

                var request = new AiChatRequest
                {
                    Messages = [Msg(ctx, "system", "You answer briefly."), summary, Msg(ctx, "user", "Name a vegetable.")],
                    Schema = EmptySchema()
                };

                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

                var body = JObject.Parse(client.LastRequestBody);
                Assert.Equal("You answer briefly.", (string)body["system"]);

                var messages = (JArray)body["messages"];
                Assert.Equal(2, messages.Count);
                Assert.Equal("assistant", (string)messages[0]["role"]);
                Assert.Contains("apples are red", (string)messages[0]["content"][0]["text"]);
            });
        }

        // ---- response parsing + thinking ------------------------------------------------------------------------

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Response_ParsesToolUse_AndDropsThinking()
        {
            await WithClient(_ => Ok(ToolUseWithThinkingResponse), async (client, ctx) =>
            {
                var response = await client.CompleteAsync(ctx, new AiChatRequest { Messages = [Msg(ctx, "user", "weather?")], Schema = EmptySchema() },
                    new AiUsage(), null, CancellationToken.None);

                Assert.Equal(AiResponseType.Tool, response.Type);
                Assert.Single(response.ToolCalls);
                Assert.Equal("toolu_1", response.ToolCalls[0].Id);
                Assert.Equal("get_weather", response.ToolCalls[0].Name);
                Assert.Contains("Paris", response.ToolCalls[0].Arguments);

                Assert.DoesNotContain("sig123", response.Message.ToString());
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task ToolUse_NextRequest_SendsTheToolCallBack_WithoutThinking()
        {
            await WithClient(
                body => body.Contains("tool_result") ? Ok(TextResponse) : Ok(ToolUseWithThinkingResponse),
                async (client, ctx) =>
                {
                    var first = await client.CompleteAsync(ctx,
                        new AiChatRequest { Messages = [Msg(ctx, "user", "weather?")], Schema = EmptySchema() },
                        new AiUsage(), null, CancellationToken.None);
                    Assert.Equal(AiResponseType.Tool, first.Type);

                    var persistedAssistant = ctx.Sync.ReadForMemory(first.Message.ToString(), "persisted/assistant");

                    await client.CompleteAsync(ctx, new AiChatRequest
                    {
                        Messages = [Msg(ctx, "user", "weather?"), persistedAssistant, Msg(ctx, "tool", "sunny, 20C", toolCallId: "toolu_1")],
                        Schema = EmptySchema()
                    }, new AiUsage(), null, CancellationToken.None);

                    var messages = (JArray)JObject.Parse(client.LastRequestBody)["messages"];

                    var assistantContent = (JArray)messages.First(m => (string)m["role"] == "assistant")["content"];
                    var toolUse = Assert.Single(assistantContent);
                    Assert.Equal("tool_use", (string)toolUse["type"]);
                    Assert.Equal("toolu_1", (string)toolUse["id"]);
                    Assert.Equal("get_weather", (string)toolUse["name"]);
                    Assert.Equal("Paris", (string)toolUse["input"]["city"]);

                    var toolResult = (JArray)messages.Last(m => (string)m["role"] == "user" &&
                                                                m["content"] is JArray c && c.Count > 0 && (string)c[0]["type"] == "tool_result")["content"];
                    Assert.Equal("toolu_1", (string)toolResult[0]["tool_use_id"]);
                    Assert.Equal("sunny, 20C", (string)toolResult[0]["content"]);
                });
        }

        // ---- error mapping ---------------------------------------------------------------------------------------

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Error_429_MapsToRateLimit_HonoringRetryAfter()
        {
            await WithClient(_ => Error(HttpStatusCode.TooManyRequests, "rate_limit_error", "slow down", ("retry-after", "30")), async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAsync<RateLimitException>(() =>
                    client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None));
                Assert.Equal(TimeSpan.FromSeconds(30), ex.RetryAfter);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Error_429_RetryAfterAsHttpDate_IsAlsoHonored()
        {
            var date = DateTimeOffset.UtcNow.AddSeconds(45).ToString("R", CultureInfo.InvariantCulture);

            await WithClient(_ => Error(HttpStatusCode.TooManyRequests, "rate_limit_error", "slow down", ("retry-after", date)), async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAsync<RateLimitException>(() =>
                    client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None));

                Assert.InRange(ex.RetryAfter, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(60));
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Error_429_WithoutRetryAfterHeader_BacksOffLikeAnExhaustedQuota()
        {
            // the monthly spend cap answers with a 429 and no retry-after, and keeps failing until it resets
            await WithClient(_ => Error(HttpStatusCode.TooManyRequests, "rate_limit_error", "slow down"), async (client, ctx) =>
            {
                await Assert.ThrowsAsync<InsufficientQuotaException>(() =>
                    client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None));
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Error_529_MapsToOverloaded_NotRateLimit()
        {
            await WithClient(_ => Error((HttpStatusCode)529, "overloaded_error", "overloaded", ("retry-after", "10")), async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAsync<UnsuccessfulAiRequestException>(() =>
                    client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None));
                Assert.IsNotType<RateLimitException>(ex);
                Assert.Equal((HttpStatusCode)529, ex.StatusCode);
                Assert.StartsWith("Status Code: 529, Message: ", ex.Message);
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData("max_tokens", false)]
        [InlineData("max_tokens", true)]
        [InlineData("model_context_window_exceeded", false)]
        [InlineData("model_context_window_exceeded", true)]
        public async Task Error_OutOfRoomStopReason_MapsToTooManyTokens(string stopReason, bool streaming)
        {
            var truncated = (streaming
                ? Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":3}}}""") +
                  Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}""") +
                  Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"{\"Ans"}}""") +
                  Event("message_delta", """{"type":"message_delta","delta":{"stop_reason":"STOP_REASON"},"usage":{"output_tokens":9}}""") +
                  Event("message_stop", """{"type":"message_stop"}""")
                : """{"id":"m","type":"message","role":"assistant","content":[{"type":"text","text":"{\"Ans"}],"stop_reason":"STOP_REASON","usage":{"input_tokens":1,"output_tokens":1}}""")
                .Replace("STOP_REASON", stopReason);

            await WithClient(_ => streaming ? Sse(truncated) : Ok(truncated), async (client, ctx) =>
                await Assert.ThrowsAsync<TooManyTokensException>(() => RunAsync(client, ctx, Simple(ctx), streaming)));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task PlainText_CutAtMaxTokens_ReturnsThePartialText(bool streaming)
        {
            var cut = streaming
                ? Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":3}}}""") +
                  Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}""") +
                  Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Once upon a"}}""") +
                  Event("message_delta", """{"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":3}}""") +
                  Event("message_stop", """{"type":"message_stop"}""")
                : """{"id":"m","type":"message","role":"assistant","content":[{"type":"text","text":"Once upon a"}],"stop_reason":"max_tokens","usage":{"input_tokens":1,"output_tokens":3}}""";

            await WithClient(_ => streaming ? Sse(cut) : Ok(cut), async (client, ctx) =>
            {
                var response = await RunAsync(client, ctx, new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = null }, streaming);

                Assert.Equal(AiResponseType.Result, response.Type);
                Assert.Equal("Once upon a", response.Result.ToString());
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Error_PromptTooLong400_WithRetryAfter_IsStillTooManyTokens()
        {
            await WithClient(_ => Error(HttpStatusCode.BadRequest, "invalid_request_error", "prompt is too long: 250000 tokens > 200000 maximum", ("retry-after", "30")), async (client, ctx) =>
                await Assert.ThrowsAsync<TooManyTokensException>(() =>
                    client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None)));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Error_UsageLimit400_StaysUnsuccessful_NotTooManyTokens()
        {
            await WithClient(_ => Error(HttpStatusCode.BadRequest, "invalid_request_error", "You have reached your specified API usage limits. You will regain access on 2026-09-01 at 00:00 UTC."), async (client, ctx) =>
                await Assert.ThrowsAsync<UnsuccessfulAiRequestException>(() =>
                    client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None)));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Response_NoContent_CarriesTheAnthropicRequestId()
        {
            const string noContent = """{"id":"m","type":"message","role":"assistant","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}""";
            await WithClient(_ =>
            {
                var r = Ok(noContent);
                r.Headers.TryAddWithoutValidation("request-id", "req_test_123");
                return r;
            }, async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAsync<UnexpectedResponseException>(() =>
                    client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None));

                Assert.Equal("req_test_123", ex.RequestId);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Response_ToolUseWithoutId_ThrowsUnexpectedResponse()
        {
            const string noId = """{"id":"m","type":"message","role":"assistant","content":[{"type":"tool_use","name":"get_weather","input":{"city":"Paris"}}],"stop_reason":"tool_use","usage":{"input_tokens":1,"output_tokens":1}}""";
            await WithClient(_ => Ok(noId), async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAsync<UnexpectedResponseException>(() =>
                    client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None));

                Assert.Contains("Invalid function call", ex.Message);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Error_RefusalStopReason_MapsToRefusedToAnswer()
        {
            const string refusal = """{"id":"m","type":"message","role":"assistant","content":[{"type":"text","text":"I can't help with that"}],"stop_reason":"refusal","usage":{"input_tokens":1,"output_tokens":1}}""";
            await WithClient(_ => Ok(refusal), async (client, ctx) =>
                await Assert.ThrowsAsync<RefusedToAnswerException>(() =>
                    client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None)));
        }

        // ---- streaming -------------------------------------------------------------------------------------------

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_ProseAlongsideToolUse_DoesNotReachTheAnswerParser()
        {
            var sse =
                Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":5}}}""") +
                Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Let me look "}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"that up."}}""") +
                Event("content_block_stop", """{"type":"content_block_stop","index":0}""") +
                Event("content_block_start", """{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_9","name":"get_weather","input":{}}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"city\":\"Paris\"}"}}""") +
                Event("content_block_stop", """{"type":"content_block_stop","index":1}""") +
                Event("message_delta", """{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":8}}""") +
                Event("message_stop", """{"type":"message_stop"}""");

            await WithClient(_ => Sse(sse), async (client, ctx) =>
            {
                using var streamed = new MemoryStream();
                var response = await client.StreamingCompleteAsync(ctx, client.Pool, "Answer",
                    new AiChatRequest { Messages = [Msg(ctx, "user", "weather?")], Schema = EmptySchema() },
                    m => { streamed.Write(m.Span); return Task.CompletedTask; }, new AiUsage(), null, CancellationToken.None);

                Assert.Equal(AiResponseType.Tool, response.Type);
                Assert.Single(response.ToolCalls);
                Assert.Empty(streamed.ToArray()); // prose was never streamed as the answer
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_ProseEndingTheTurn_FailsAsInvalidStructuredResponse()
        {
            var sse =
                Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":5}}}""") +
                Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Let me look that up."}}""") +
                Event("content_block_stop", """{"type":"content_block_stop","index":0}""") +
                Event("message_delta", """{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":8}}""") +
                Event("message_stop", """{"type":"message_stop"}""");

            await WithClient(_ => Sse(sse), async (client, ctx) =>
            {
                using var streamed = new MemoryStream();
                var ex = await Assert.ThrowsAnyAsync<Exception>(() => client.StreamingCompleteAsync(ctx, client.Pool, "Answer",
                    new AiChatRequest { Messages = [Msg(ctx, "user", "weather?")], Schema = EmptySchema() },
                    m => { streamed.Write(m.Span); return Task.CompletedTask; }, new AiUsage(), null, CancellationToken.None));

                Assert.IsType<UnexpectedResponseException>(ex);
                Assert.Contains("not valid JSON", ex.Message);
                Assert.Contains("Let me look that up.", ex.Message);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_JsonAnswerWithLeadingWhitespace_StillStreams()
        {
            var sse =
                Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":5}}}""") +
                Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"  \n"}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"{\"Answer\":\"hi\"}"}}""") +
                Event("content_block_stop", """{"type":"content_block_stop","index":0}""") +
                Event("message_delta", """{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}""") +
                Event("message_stop", """{"type":"message_stop"}""");

            await WithClient(_ => Sse(sse), async (client, ctx) =>
            {
                using var streamed = new MemoryStream();
                var response = await client.StreamingCompleteAsync(ctx, client.Pool, "Answer",
                    new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = EmptySchema() },
                    m => { streamed.Write(m.Span); return Task.CompletedTask; }, new AiUsage(), null, CancellationToken.None);

                Assert.Equal(AiResponseType.Result, response.Type);
                Assert.True(((BlittableJsonReaderObject)response.Result).TryGet("Answer", out string answer));
                Assert.Equal("hi", answer);
                Assert.Equal("hi", Encoding.UTF8.GetString(streamed.ToArray()));
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_FragmentedInputJsonDelta_ReassemblesToolArguments()
        {
            var sse =
                Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":3}}}""") +
                Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_frag","name":"get_weather","input":{}}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"ci"}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"ty\":\"Par"}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"is\",\"un"}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"it\":\"c\"}"}}""") +
                Event("content_block_stop", """{"type":"content_block_stop","index":0}""") +
                Event("message_delta", """{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":6}}""") +
                Event("message_stop", """{"type":"message_stop"}""");

            await WithClient(_ => Sse(sse), async (client, ctx) =>
            {
                using var streamed = new MemoryStream();
                var response = await client.StreamingCompleteAsync(ctx, client.Pool, "Answer",
                    new AiChatRequest { Messages = [Msg(ctx, "user", "weather?")], Schema = EmptySchema() },
                    m => { streamed.Write(m.Span); return Task.CompletedTask; }, new AiUsage(), null, CancellationToken.None);

                Assert.Equal(AiResponseType.Tool, response.Type);
                Assert.Single(response.ToolCalls);
                var args = JObject.Parse(response.ToolCalls[0].Arguments); // parses => valid JSON, fully reassembled
                Assert.Equal("Paris", (string)args["city"]);
                Assert.Equal("c", (string)args["unit"]);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_ErrorEvent_ThrowsUnsuccessfulMidStream()
        {
            var sse =
                Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":3}}}""") +
                Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}""") +
                Event("error", """{"type":"error","error":{"type":"overloaded_error","message":"the server is overloaded"}}""");

            await WithClient(_ => SseWithHeaders(sse, ("retry-after", "7")), async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAsync<UnsuccessfulAiRequestException>(() =>
                    client.StreamingCompleteAsync(ctx, client.Pool, "Answer",
                        new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = EmptySchema() },
                        _ => Task.CompletedTask, new AiUsage(), null, CancellationToken.None));
                Assert.Contains("the server is overloaded", ex.Message);
                Assert.Equal((HttpStatusCode)529, ex.StatusCode);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_AggregatesUsageAcrossStartAndDeltaEvents()
        {
            var sse =
                Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":100,"cache_read_input_tokens":20,"cache_creation_input_tokens":5}}}""") +
                Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"{\"Answer\":\"ok\"}"}}""") +
                Event("content_block_stop", """{"type":"content_block_stop","index":0}""") +
                Event("message_delta", """{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":50,"output_tokens_details":{"thinking_tokens":30}}}""") +
                Event("message_stop", """{"type":"message_stop"}""");

            await WithClient(_ => Sse(sse), async (client, ctx) =>
            {
                var usage = new AiUsage();
                await client.StreamingCompleteAsync(ctx, client.Pool, "Answer",
                    new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = EmptySchema() },
                    _ => Task.CompletedTask, usage, null, CancellationToken.None);

                Assert.Equal(125, usage.PromptTokens);      // 100 + 20 + 5
                Assert.Equal(20, usage.CachedTokens);        // cache_read
                Assert.Equal(50, usage.CompletionTokens);    // output
                Assert.Equal(175, usage.TotalTokens);        // prompt + completion
                Assert.Equal(30, usage.ReasoningTokens);     // part of output
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_CumulativeUsageInSeveralMessageDeltas_IsCountedOnce()
        {
            var sse =
                Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":10}}}""") +
                Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"{\"Answer\":\"ok\"}"}}""") +
                Event("content_block_stop", """{"type":"content_block_stop","index":0}""") +
                Event("message_delta", """{"type":"message_delta","delta":{},"usage":{"output_tokens":20,"output_tokens_details":{"thinking_tokens":5}}}""") +
                Event("message_delta", """{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":50,"output_tokens_details":{"thinking_tokens":12}}}""") +
                Event("message_stop", """{"type":"message_stop"}""");

            await WithClient(_ => Sse(sse), async (client, ctx) =>
            {
                var usage = new AiUsage();
                await client.StreamingCompleteAsync(ctx, client.Pool, "Answer",
                    new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = EmptySchema() },
                    _ => Task.CompletedTask, usage, null, CancellationToken.None);

                Assert.Equal(50, usage.CompletionTokens);
                Assert.Equal(12, usage.ReasoningTokens);
                Assert.Equal(60, usage.TotalTokens);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_TruncatedStream_MidToolUse_DoesNotExecuteWithEmptyArguments()
        {
            var sse =
                Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":3}}}""") +
                Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{}}}""");

            await WithClient(_ => Sse(sse), async (client, ctx) =>
            {
                var ex = await StreamAndCaptureError(client, ctx);
                Assert.IsType<UnexpectedResponseException>(ex);
                Assert.Contains("message_stop", ex.Message);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streamed_And_NonStreamed_ToolUse_PersistTheSameMessageShape()
        {
            var streamedSse =
                Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":20}}}""") +
                Event("content_block_start", """{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"let me think"}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig123"}}""") +
                Event("content_block_stop", """{"type":"content_block_stop","index":0}""") +
                Event("content_block_start", """{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{}}}""") +
                Event("content_block_delta", """{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"city\":\"Paris\"}"}}""") +
                Event("content_block_stop", """{"type":"content_block_stop","index":1}""") +
                Event("message_delta", """{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":8}}""") +
                Event("message_stop", """{"type":"message_stop"}""");

            JObject nonStreamed = null, streamed = null;

            await WithClient(_ => Ok(ToolUseWithThinkingResponse), async (client, ctx) =>
            {
                var r = await client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None);
                nonStreamed = JObject.Parse(r.Message.ToString());
            });

            await WithClient(_ => Sse(streamedSse), async (client, ctx) =>
            {
                var r = await client.StreamingCompleteAsync(ctx, client.Pool, "Answer",
                    new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = EmptySchema() },
                    _ => Task.CompletedTask, new AiUsage(), null, CancellationToken.None);
                streamed = JObject.Parse(r.Message.ToString());
            });

            foreach (var message in new[] { nonStreamed, streamed })
            {
                Assert.Equal("assistant", (string)message["role"]);
                Assert.True(message["content"] == null || message["content"].Type == JTokenType.Null);
                Assert.Equal("toolu_1", (string)message["tool_calls"][0]["id"]);
                Assert.Equal("get_weather", (string)message["tool_calls"][0]["function"]["name"]);
                Assert.Equal("function", (string)message["tool_calls"][0]["type"]);
                Assert.Equal(["role", "content", "tool_calls"], message.Properties().Select(p => p.Name));
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_RateLimitErrorEvent_WithoutRetryAfter_BacksOffLikeAnExhaustedQuota()
        {
            await WithClient(_ => SseWithHeaders(ErrorEvent("rate_limit_error", "slow down")), async (client, ctx) =>
            {
                Assert.IsType<InsufficientQuotaException>(await StreamAndCaptureError(client, ctx));
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData("authentication_error", HttpStatusCode.Unauthorized)]
        [InlineData("permission_error", HttpStatusCode.Forbidden)]
        [InlineData("invalid_request_error", HttpStatusCode.BadRequest)]
        [InlineData("not_found_error", HttpStatusCode.NotFound)]
        public async Task Streaming_PermanentErrorEvent_IsNotRetryable(string errorType, HttpStatusCode expected)
        {
            await WithClient(_ => SseWithHeaders(ErrorEvent(errorType, "nope"), ("retry-after", "30")), async (client, ctx) =>
            {
                var ex = Assert.IsType<UnsuccessfulAiRequestException>(await StreamAndCaptureError(client, ctx));

                Assert.Equal(expected, ex.StatusCode);
                Assert.Contains(errorType, ex.Message);
                Assert.Contains("nope", ex.Message);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Streaming_UnknownErrorEvent_PreservesTypeAndMessage_AndIsNotAssumedRetryable()
        {
            await WithClient(_ => SseWithHeaders(ErrorEvent("some_future_error", "who knows"), ("retry-after", "30")), async (client, ctx) =>
            {
                var ex = Assert.IsType<UnsuccessfulAiRequestException>(await StreamAndCaptureError(client, ctx));

                Assert.Contains("some_future_error", ex.Message);
                Assert.Contains("who knows", ex.Message);
            });
        }

        // ---- attachments -----------------------------------------------------------------------------------------

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_TranslatesAttachments_IntoImageDocumentAndTextBlocks()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var request = new AiChatRequest
                {
                    Messages = [Msg(ctx, "user", "look at these")],
                    Attachments =
                    [
                        new AiAttachment("pic.png", "image/png", AiAttachmentSource.FromAttachment, "aW1n"),
                        new AiAttachment("doc.pdf", "application/pdf", AiAttachmentSource.FromAttachment, "cGRm"),
                        new AiAttachment("notes.txt", "text/plain", AiAttachmentSource.FromAttachment, "hello notes")
                    ],
                    Schema = EmptySchema()
                };

                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

                var messages = (JArray)JObject.Parse(client.LastRequestBody)["messages"];
                var blocks = (JArray)messages.Last(m => (string)m["role"] == "user")["content"];

                var image = blocks.First(b => (string)b["type"] == "image");
                Assert.Equal("base64", (string)image["source"]["type"]);
                Assert.Equal("image/png", (string)image["source"]["media_type"]);
                Assert.Equal("aW1n", (string)image["source"]["data"]);

                var doc = blocks.First(b => (string)b["type"] == "document");
                Assert.Equal("application/pdf", (string)doc["source"]["media_type"]);
                Assert.Equal("cGRm", (string)doc["source"]["data"]);

                var text = blocks.First(b => (string)b["type"] == "text");
                Assert.Equal("hello notes", (string)text["text"]);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_NotFoundAttachment_BecomesTextNote()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var request = new AiChatRequest
                {
                    Messages = [Msg(ctx, "user", "look")],
                    Attachments = [new AiAttachment("missing.png", "image/png", AiAttachmentSource.NotFound, null)],
                    Schema = EmptySchema()
                };

                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

                var messages = (JArray)JObject.Parse(client.LastRequestBody)["messages"];
                var blocks = (JArray)messages.Last(m => (string)m["role"] == "user")["content"];
                Assert.Contains(blocks, b => (string)b["type"] == "text" && ((string)b["text"]).Contains("could not be loaded"));
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_UnknownAttachmentType_Throws()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var request = new AiChatRequest
                {
                    Messages = [Msg(ctx, "user", "look")],
                    Attachments = [new AiAttachment("archive.zip", "application/zip", AiAttachmentSource.FromAttachment, "emlw")],
                    Schema = EmptySchema()
                };

                await Assert.ThrowsAsync<InvalidOperationException>(() =>
                    client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None));
            });
        }

        // ---- extra request / response cases ----------------------------------------------------------------------

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Request_JoinsMultipleSystemMessages()
        {
            await WithClient(_ => Ok(TextResponse), async (client, ctx) =>
            {
                var request = new AiChatRequest
                {
                    Messages = [Msg(ctx, "system", "First rule."), Msg(ctx, "system", "Second rule."), Msg(ctx, "user", "hi")],
                    Schema = EmptySchema()
                };

                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

                var system = (string)JObject.Parse(client.LastRequestBody)["system"];
                Assert.Contains("First rule.", system);
                Assert.Contains("Second rule.", system);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Response_ProseAlongsideToolUse_IsNotParsedAsTheAnswer()
        {
            const string json = """
                {"id":"msg_1","type":"message","role":"assistant","stop_reason":"tool_use",
                 "content":[{"type":"text","text":"Let me look that up."},
                            {"type":"tool_use","id":"toolu_3","name":"get_weather","input":{"city":"Paris"}}],
                 "usage":{"input_tokens":5,"output_tokens":8}}
                """;

            await WithClient(_ => Ok(json), async (client, ctx) =>
            {
                var response = await client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None);

                Assert.Equal(AiResponseType.Tool, response.Type);
                Assert.Single(response.ToolCalls);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Response_ProseEndingTheTurn_FailsAsInvalidStructuredResponse()
        {
            const string json = """
                {"id":"msg_1","type":"message","role":"assistant","stop_reason":"end_turn",
                 "content":[{"type":"text","text":"Let me look that up."}],
                 "usage":{"input_tokens":5,"output_tokens":8}}
                """;

            await WithClient(_ => Ok(json), async (client, ctx) =>
            {
                var ex = await Assert.ThrowsAnyAsync<Exception>(() =>
                    client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None));

                Assert.IsType<UnexpectedResponseException>(ex);
                Assert.Contains("not valid JSON", ex.Message);
                Assert.Contains("Let me look that up.", ex.Message);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Response_MultipleToolUseBlocks_ProduceMultipleToolCalls()
        {
            const string twoTools =
                """
                {"id":"m","type":"message","role":"assistant","content":[
                  {"type":"tool_use","id":"t1","name":"get_weather","input":{"city":"Paris"}},
                  {"type":"tool_use","id":"t2","name":"get_time","input":{"tz":"CET"}}],
                "stop_reason":"tool_use","usage":{"input_tokens":5,"output_tokens":3}}
                """;
            await WithClient(_ => Ok(twoTools), async (client, ctx) =>
            {
                var response = await client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None);
                Assert.Equal(AiResponseType.Tool, response.Type);
                Assert.Equal(2, response.ToolCalls.Count);
                Assert.Equal("get_weather", response.ToolCalls[0].Name);
                Assert.Equal("get_time", response.ToolCalls[1].Name);
            });
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Response_ThinkingBlockInTextAnswer_IsIgnored_NotInResult()
        {
            const string thinkingThenText =
                """
                {"id":"m","type":"message","role":"assistant","content":[
                  {"type":"thinking","thinking":"the user greeted me, secret reasoning","signature":"s"},
                  {"type":"text","text":"{\"Answer\":\"hello\"}"}],
                "stop_reason":"end_turn","usage":{"input_tokens":5,"output_tokens":3}}
                """;
            await WithClient(_ => Ok(thinkingThenText), async (client, ctx) =>
            {
                var response = await client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None);
                Assert.Equal(AiResponseType.Result, response.Type);
                Assert.True(((BlittableJsonReaderObject)response.Result).TryGet("Answer", out string answer));
                Assert.Equal("hello", answer);
                Assert.DoesNotContain("secret reasoning", response.Result.ToString()); // thinking never reaches the answer
            });
        }

        // ---- image-input capability probe --------------------------------------------------------------------------

        private const string PlainTextResponse =
            """
            {"id":"msg_p","type":"message","role":"assistant","content":[{"type":"text","text":"A tiny red square on a white background."}],"stop_reason":"end_turn","usage":{"input_tokens":9,"output_tokens":11}}
            """;

        [RavenFact(RavenTestCategory.Ai)]
        public async Task ImageProbe_AsksForNoOutputFormat_AndLeavesNormalRequestsStructured()
        {
            await WithClient(body => Ok(body.Contains("output_config") ? TextResponse : PlainTextResponse), async (client, ctx) =>
            {
                Assert.True(await client.TestAcceptsImageInputAsync(CancellationToken.None));
                var probe = JObject.Parse(client.LastRequestBody);
                Assert.Contains((JArray)((JArray)probe["messages"]).Last(m => (string)m["role"] == "user")["content"], b => (string)b["type"] == "image");
                Assert.Null(probe["output_config"]?["format"]);

                var structured = await client.CompleteAsync(ctx, Simple(ctx), new AiUsage(), null, CancellationToken.None);
                Assert.NotNull(JObject.Parse(client.LastRequestBody)["output_config"]["format"]);
                Assert.IsAssignableFrom<BlittableJsonReaderObject>(structured.Result);

                var plain = await client.CompleteAsync(ctx, new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = null },
                    new AiUsage(), null, CancellationToken.None);
                Assert.Null(JObject.Parse(client.LastRequestBody)["output_config"]);
                Assert.IsType<string>(plain.Result);
            });
        }

        // ---- helpers ---------------------------------------------------------------------------------------------

        private static Task<AiResponse> RunAsync(CapturingChatCompletionClient client, JsonOperationContext ctx, AiChatRequest request, bool streaming)
        {
            if (streaming == false)
                return client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

            return client.StreamingCompleteAsync(ctx, client.Pool, "Answer", request,
                _ => Task.CompletedTask, new AiUsage(), null, CancellationToken.None);
        }

        private static Task WithClient(Func<string, HttpResponseMessage> respond, Func<CapturingChatCompletionClient, JsonOperationContext, Task> body) =>
            WithClient(new AnthropicSettings("sk-ant-test", "claude-opus-4-8", "https://api.anthropic.com/v1/"), respond, body);

        private static async Task WithClient(AnthropicSettings settings, Func<string, HttpResponseMessage> respond, Func<CapturingChatCompletionClient, JsonOperationContext, Task> body)
        {
            using var storageEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests());
            using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), storageEnv);
            using var client = new CapturingChatCompletionClient(contextPool, new AnthropicChatCompletionProvider(settings), respond);
            using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
                await body(client, ctx);
        }

        private static string EmptySchema() => ChatCompletionClient.EmptySchema;

        private static AiChatRequest Simple(JsonOperationContext ctx) => new() { Messages = [Msg(ctx, "user", "hi")], Schema = ChatCompletionClient.EmptySchema };

        private static BlittableJsonReaderObject Msg(JsonOperationContext ctx, string role, string content, string toolCallId = null)
        {
            var djv = new DynamicJsonValue { ["role"] = role, ["content"] = content };
            if (toolCallId != null)
                djv["tool_call_id"] = toolCallId;
            return ctx.ReadObject(djv, "msg");
        }

        private static DynamicJsonValue ToolCall(string id, string name, string arguments) => new()
        {
            ["id"] = id,
            ["type"] = "function",
            ["function"] = new DynamicJsonValue { ["name"] = name, ["arguments"] = arguments }
        };

        private static HttpResponseMessage SseWithHeaders(string sse, params (string Name, string Value)[] headers)
        {
            var response = Sse(sse);
            foreach (var (name, value) in headers)
                response.Headers.TryAddWithoutValidation(name, value);
            return response;
        }

        private static HttpResponseMessage Error(HttpStatusCode code, string type, string message, (string Name, string Value)? header = null)
        {
            var response = new HttpResponseMessage(code)
            {
                Content = new StringContent($"{{\"type\":\"error\",\"error\":{{\"type\":\"{type}\",\"message\":\"{message}\"}}}}", Encoding.UTF8, "application/json")
            };
            if (header != null)
                response.Headers.TryAddWithoutValidation(header.Value.Name, header.Value.Value);
            return response;
        }

        private static string Event(string name, string data) => $"event: {name}\ndata: {data}\n\n";

        private static string ErrorEvent(string type, string message) =>
            Event("message_start", """{"type":"message_start","message":{"usage":{"input_tokens":3}}}""") +
            Event("error", $"{{\"type\":\"error\",\"error\":{{\"type\":\"{type}\",\"message\":\"{message}\"}}}}");

        private static async Task<Exception> StreamAndCaptureError(CapturingChatCompletionClient client, JsonOperationContext ctx)
        {
            using var streamed = new MemoryStream();
            return await Assert.ThrowsAnyAsync<Exception>(() => client.StreamingCompleteAsync(ctx, client.Pool, "Answer",
                new AiChatRequest { Messages = [Msg(ctx, "user", "hi")], Schema = EmptySchema() },
                m => { streamed.Write(m.Span); return Task.CompletedTask; }, new AiUsage(), null, CancellationToken.None));
        }
    }
}
