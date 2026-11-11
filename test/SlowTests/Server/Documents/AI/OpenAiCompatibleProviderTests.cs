using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;
using static SlowTests.Server.Documents.AI.CapturingChatCompletionClient;

namespace SlowTests.Server.Documents.AI
{
    public class OpenAiCompatibleProviderTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenFact(RavenTestCategory.Ai)]
        public async Task OpenAiFamily_MalformedResponse_CarriesTheRequestId()
        {
            using var contextPool = NewContextPool();
            using var client = NewClient(OpenAi(), _ =>
            {
                var r = Ok("{}"); // no choices -> malformed response
                r.Headers.TryAddWithoutValidation("X-Request-ID", "oai_req_9");
                return r;
            }, contextPool);

            using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                var ex = await Assert.ThrowsAsync<UnexpectedResponseException>(() =>
                    client.CompleteAsync(ctx,
                        new AiChatRequest { Messages = [UserMessage(ctx, "hi")], Schema = ChatCompletionClient.EmptySchema },
                        new AiUsage(), null, CancellationToken.None));

                Assert.Equal("oai_req_9", ex.RequestId);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task OpenAiFamily_Schema_IsParsedOncePerConversationCall_AndTheBodyIsUnchanged()
        {
            using var contextPool = NewContextPool();
            using var client = NewClient(OpenAi(), _ => Ok(AiAgent.MockLlm.CreateAnswerResponse("\"x\"")), contextPool);

            using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                var messages = new[] { UserMessage(ctx, "hi") };
                await client.CompleteAsync(ctx, new AiChatRequest { Messages = messages, Schema = ChatCompletionClient.EmptySchema }, new AiUsage(), null, CancellationToken.None);
                var bodyWithoutCache = client.LastRequestBody;

                var parsedJson = new ParsedJsonCache(ctx);
                var request = new AiChatRequest { Messages = messages, Schema = ChatCompletionClient.EmptySchema, ParsedJson = parsedJson };
                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);
                await client.CompleteAsync(ctx, request, new AiUsage(), null, CancellationToken.None);

                Assert.Equal(bodyWithoutCache, client.LastRequestBody);
                Assert.Equal(1, parsedJson.ParseCount);
            }
        }

        // Chat Completions reads strict only inside "function" (RavenDB-27249)
        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(AiConnectorType.OpenAi, true)]
        [InlineData(AiConnectorType.AzureOpenAi, true)]
        [InlineData(AiConnectorType.Google, false)]
        public void Tools_StrictFlagGoesInsideFunction(AiConnectorType type, bool supportsStrict)
        {
            var provider = type switch
            {
                AiConnectorType.OpenAi => OpenAi(),
                AiConnectorType.AzureOpenAi => new AzureOpenAiChatCompletionProvider(new AzureOpenAiSettings("key", "https://test.openai.azure.com/", "gpt-test", "deployment")),
                _ => new GoogleChatCompletionProvider(new GoogleSettings("gemini-test", "key"))
            };

            using var contextPool = NewContextPool();
            using var client = NewClient(provider, _ => Ok("{}"), contextPool);

            using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                var generatedSchema = ChatCompletionClient.GetSchemaForTool(schema: null, sampleObject: "{\"id\": \"customers/1\"}");
                const string optedOut = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":true}";
                var tools = client.PrepareTools(ctx, [new AiToolDescriptor("generated", "d", generatedSchema), new AiToolDescriptor("user", "d", optedOut)]);

                Assert.All(tools, tool => Assert.False(tool.TryGetMember("strict", out _)));

                tools[0].TryGet("function", out BlittableJsonReaderObject generated);
                Assert.Equal(supportsStrict, generated.TryGet("strict", out bool strict) && strict);

                tools[1].TryGet("function", out BlittableJsonReaderObject user);
                Assert.False(user.TryGetMember("strict", out _));
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public void CanUseStrictTool_OnlyWhenEveryObjectIsClosedAndFullyRequired()
        {
            var generated = ChatCompletionClient.GetSchemaForTool(schema: null, "{\"id\": \"customers/1\", \"lines\": [{\"product\": \"Chai\", \"quantity\": 1}]}");

            // the schemas we generate close every object and require every property
            Assert.True(CanUseStrictTool(generated));
            Assert.True(CanUseStrictTool(ChatCompletionClient.GetSchemaForTool(schema: null, "{\"additionalProperties\": \"a tool argument with that name\"}")));
            using (var contextPool = NewContextPool())
            using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                Assert.True(CanUseStrictTool(ConversationHandler.GetSchemaForSubAgentTool(ctx, new Dictionary<string, ConversationHandler.ParameterDefinition>
                {
                    ["userPrompt"] = new("what the sub-agent should do", AiAgentParameterValueType.String),
                    ["tags"] = new("tags", AiAgentParameterValueType.ArrayOfString)
                })));
            }

            // a schema the user wrote is strict only when every object has additionalProperties: false and all its properties required
            Assert.True(CanUseStrictTool("{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}"));
            Assert.False(CanUseStrictTool("{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"]}"));
            Assert.False(CanUseStrictTool(
                "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"filter\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"string\"}},\"required\":[\"status\"]}},\"required\":[\"filter\"]}"));
            Assert.False(CanUseStrictTool("{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}}}"));
            Assert.True(CanUseStrictTool("{\"type\":\"object\",\"properties\":{},\"additionalProperties\":false}"));
            Assert.False(CanUseStrictTool("{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[]}"));
            Assert.False(CanUseStrictTool(
                "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"filter\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"status\":{\"type\":\"string\"}}}},\"required\":[\"filter\"]}"));
            Assert.False(CanUseStrictTool("{\"type\":\"object\",\"additionalProperties\":true}"));
            Assert.False(CanUseStrictTool(generated.Replace("\"additionalProperties\": false", "\"additionalProperties\": {\"type\":\"string\"}")));
            Assert.False(CanUseStrictTool(
                "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"filter\":{\"type\":\"object\",\"additionalProperties\":true}},\"required\":[\"filter\"]}"));
            Assert.False(CanUseStrictTool(
                "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"lines\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true}}},\"required\":[\"lines\"]}"));
            // only "type": "object" / "array" are followed; an object under a type list, anyOf or without "type" isn't checked,
            // so the tool goes strict as written and the user has to close those objects
            Assert.True(CanUseStrictTool("{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"filter\":{\"type\":[\"object\",\"null\"]}},\"required\":[\"filter\"]}"));
            Assert.True(CanUseStrictTool("{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"filter\":{\"additionalProperties\":true}},\"required\":[\"filter\"]}"));
            Assert.True(CanUseStrictTool(
                "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"lines\":{\"type\":[\"array\",\"null\"],\"items\":{\"type\":\"object\",\"additionalProperties\":true}}},\"required\":[\"lines\"]}"));
            Assert.True(CanUseStrictTool(
                "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"v\":{\"anyOf\":[{\"type\":\"string\"},{\"type\":\"object\",\"additionalProperties\":true}]}},\"required\":[\"v\"]}"));
            // a duplicate key counts with its last value, same as in the request
            Assert.True(CanUseStrictTool("{\"type\":\"object\",\"additionalProperties\":true,\"additionalProperties\":false}"));
            Assert.False(CanUseStrictTool("{\"type\":\"object\",\"additionalProperties\":false,\"additionalProperties\":true}"));
        }

        private static bool CanUseStrictTool(string schema)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
                return ChatCompletionClient.CanUseStrictTool(context.Sync.ReadForMemory(schema, "schema"));
        }

        // The client only allocates a JsonOperationContext, so a plain pool is enough (as in BaseAiConnectorForTesting).
        private static JsonContextPool NewContextPool() => new();

        private static CapturingChatCompletionClient NewClient(AbstractChatCompletionProvider provider, Func<string, HttpResponseMessage> respond, IMemoryContextPool contextPool) =>
            new(contextPool, provider, respond);

        private static AbstractChatCompletionProvider OpenAi() =>
            new OpenAiChatCompletionProvider(new OpenAiSettings { ApiKey = "sk-test", Model = "gpt-test", Endpoint = "https://api.openai.com/v1/" });

        private static BlittableJsonReaderObject UserMessage(JsonOperationContext ctx, string content) =>
            ctx.ReadObject(new DynamicJsonValue { ["role"] = "user", ["content"] = content }, "msg");
    }
}
