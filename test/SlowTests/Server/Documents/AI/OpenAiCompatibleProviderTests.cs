using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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
        [InlineData(AiConnectorType.OpenAi)]
        [InlineData(AiConnectorType.AzureOpenAi)]
        [InlineData(AiConnectorType.Google)]
        public void Tools_StrictFlagGoesInsideFunction(AiConnectorType type)
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
                var schema = ChatCompletionClient.GetSchemaForTool(schema: null, sampleObject: "{\"id\": \"customers/1\"}");
                var tools = client.PrepareTools(ctx, [new AiToolDescriptor("strict", "d", schema, strict: true), new AiToolDescriptor("notStrict", "d", schema, strict: false)]);

                Assert.All(tools, tool => Assert.False(tool.TryGetMember("strict", out _)));

                tools[0].TryGet("function", out BlittableJsonReaderObject strictTool);
                Assert.True(strictTool.TryGet("strict", out bool strict) && strict);

                tools[1].TryGet("function", out BlittableJsonReaderObject notStrictTool);
                Assert.False(notStrictTool.TryGetMember("strict", out _));
            }
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
