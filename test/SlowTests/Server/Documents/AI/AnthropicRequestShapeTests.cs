using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using static SlowTests.Server.Documents.AI.CapturingChatCompletionClient;

namespace SlowTests.Server.Documents.AI
{
    public class AnthropicRequestShapeTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public async Task AnthropicAssistantTurn_WithNoUsableContent_IsSkipped(string content)
        {
            var messages = await AnthropicMessages(ctx =>
            [
                UserMessage(ctx, "hi"),
                ctx.ReadObject(new DynamicJsonValue { ["role"] = "assistant", ["content"] = content }, "assistant/msg"),
                UserMessage(ctx, "still there?")
            ]);

            Assert.Equal(2, messages.Count);
            Assert.All(messages, m => Assert.Equal("user", (string)m["role"]));
        }

        private static async Task<List<JObject>> AnthropicMessages(Func<JsonOperationContext, List<BlittableJsonReaderObject>> build)
        {
            using var pool = NewContextPool();
            using var client = NewClient(Anthropic(), contextPool: pool);

            string payload;
            using (pool.AllocateOperationContext(out JsonOperationContext ctx))
            using (var stream = new MemoryStream())
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    client.ForTestingPurposesOnly().Provider.WritePayload(writer, ctx, new AiChatRequest
                    {
                        Messages = build(ctx),
                        Schema = ChatCompletionClient.EmptySchema
                    }, streaming: false);
                    await writer.FlushAsync();
                }

                payload = Encoding.UTF8.GetString(stream.ToArray());
            }

            return ((JArray)JObject.Parse(payload)["messages"]).Cast<JObject>().ToList();
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(@"{""type"":""object"",""properties"":{""city"":{""type"":""string""}},""required"":[""city""]}")]           // root missing additionalProperties
        [InlineData(@"{""type"":""object"",""additionalProperties"":false,""properties"":{""city"":{""type"":""string""}}}")] // already closed
        public async Task AnthropicTool_ValidObjectRoot_IsClosedAndStrict(string parametersSchema)
        {
            var tool = await AnthropicTool(parametersSchema);

            Assert.True((bool)tool["strict"]);
            Assert.False((bool)tool["input_schema"]["additionalProperties"]);
            Assert.True(JToken.DeepEquals(JObject.Parse(parametersSchema)["properties"], tool["input_schema"]["properties"]));
        }

        private static async Task<JObject> AnthropicTool(string parametersSchema)
        {
            using var pool = NewContextPool();
            using var client = NewClient(Anthropic(), contextPool: pool);

            string payload;
            using (pool.AllocateOperationContext(out JsonOperationContext ctx))
            using (var stream = new MemoryStream())
            {
                var tools = client.PrepareTools(ctx, [new AiToolDescriptor("get_weather", "weather by city", parametersSchema)]);

                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    client.ForTestingPurposesOnly().Provider.WritePayload(writer, ctx, new AiChatRequest
                    {
                        Messages = [UserMessage(ctx, "hi")],
                        Tools = tools,
                        UseTools = true,
                        Schema = ChatCompletionClient.EmptySchema
                    }, streaming: false);
                    await writer.FlushAsync();
                }

                payload = Encoding.UTF8.GetString(stream.ToArray());
            }

            return (JObject)((JArray)JObject.Parse(payload)["tools"])[0];
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(null)]
        [InlineData("{}")]
        public async Task AnthropicTool_WithoutParameters_EmitsClosedEmptyObject(string parametersSchema)
        {
            var tool = await AnthropicTool(parametersSchema);
            var schema = tool["input_schema"];

            Assert.Equal("object", (string)schema["type"]);
            Assert.NotNull(schema["properties"]);
            Assert.Empty((JObject)schema["properties"]);
            Assert.False((bool)schema["additionalProperties"]);
            Assert.True((bool)tool["strict"]);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task RealSubAgentSchema_IsClosedAndStrictForAnthropic()
        {
            // The real generator emits type/properties/required and no additionalProperties.
            string canonical;
            using (var pool = NewContextPool())
            using (pool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                canonical = ConversationHandler.GetSchemaForSubAgentTool(ctx, new Dictionary<string, ConversationHandler.ParameterDefinition>
                {
                    ["userPrompt"] = new("what the sub-agent should do", AiAgentParameterValueType.String),
                    ["count"] = new("how many", AiAgentParameterValueType.Number)
                });
            }

            // 1) the provider-neutral schema is what we think it is, and is open
            var canonicalJson = JObject.Parse(canonical);
            Assert.Equal("object", (string)canonicalJson["type"]);
            Assert.Null(canonicalJson["additionalProperties"]);

            // 2) the Anthropic copy is closed and strict
            var tool = await AnthropicTool(canonical);
            Assert.True((bool)tool["strict"]);
            Assert.Equal("object", (string)tool["input_schema"]["type"]);
            Assert.False((bool)tool["input_schema"]["additionalProperties"]);
        }

        // The client only allocates a JsonOperationContext, so a plain pool is enough (as in BaseAiConnectorForTesting).
        private static JsonContextPool NewContextPool() => new();

        private static CapturingChatCompletionClient NewClient(AbstractChatCompletionProvider settings, Func<string, HttpResponseMessage> respond = null, IMemoryContextPool contextPool = null) =>
            new(contextPool ?? NewContextPool(), settings, respond ?? (_ => Ok("{}")));

        private static AbstractChatCompletionProvider Anthropic() =>
            new AnthropicChatCompletionProvider(new AnthropicSettings("sk-ant-test", "claude-opus-4-8", "https://api.anthropic.com/v1/"));

        private static BlittableJsonReaderObject UserMessage(JsonOperationContext ctx, string content) =>
            ctx.ReadObject(new DynamicJsonValue { ["role"] = "user", ["content"] = content }, "msg");
    }
}
