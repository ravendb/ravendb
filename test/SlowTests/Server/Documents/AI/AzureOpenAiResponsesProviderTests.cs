using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using SlowTests.Server.Documents.AI.AiAgent;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Tests.Infrastructure;
using Xunit;
using static SlowTests.Server.Documents.AI.CapturingChatCompletionClient;

namespace SlowTests.Server.Documents.AI
{
    public class AzureOpenAiResponsesProviderTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const string Blocked = "Response blocked due to content policy: violence (high severity)";

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData("none-blocked", null)]
        [InlineData("prompt-blocked", "prompt: " + Blocked)]
        [InlineData("completion-blocked", "completion: " + Blocked)]
        [InlineData("both-blocked", "prompt: " + Blocked + "; completion: " + Blocked)]
        [InlineData("blocked-without-category", "completion: Response blocked due to content policy")]
        [InlineData("blocked-without-source-type", "content: Response blocked due to content policy")]
        [InlineData("no-content-filters", null)]
        public void BlockedContentFilterMessage(string filters, string expected)
        {
            var json = filters switch
            {
                "none-blocked" => "{" + ResponsesWire.AzureContentFilters(false, false) + "}",
                "prompt-blocked" => "{" + ResponsesWire.AzureContentFilters(true, false) + "}",
                "completion-blocked" => "{" + ResponsesWire.AzureContentFilters(false, true) + "}",
                "both-blocked" => "{" + ResponsesWire.AzureContentFilters(true, true) + "}",
                "blocked-without-category" => "{" + ResponsesWire.AzureContentFiltersBlockedWithoutCategory() + "}",
                "blocked-without-source-type" => "{\"content_filters\":[{\"blocked\":true}]}",
                _ => "{\"id\":\"resp_1\",\"status\":\"completed\"}"
            };

            using var context = JsonOperationContext.ShortTermSingleUse();
            using var response = context.Sync.ReadForMemory(json, "response");

            Assert.Equal(expected, AzureOpenAiShared.GetBlockedContentFilterMessage(response));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(null, true)]
        [InlineData(OpenAiApiMode.Responses, true)]
        [InlineData(OpenAiApiMode.ChatCompletions, false)]
        public void TryGetParameters_Azure_PicksTheApi(OpenAiApiMode? apiMode, bool expectResponses)
        {
            var settings = ProviderSettings();
            settings.ApiMode = apiMode;
            var connection = new AiConnectionString { Name = "azure", ModelType = AiModelType.Chat, AzureOpenAiSettings = settings };

            Assert.True(AbstractChatCompletionProvider.TryGetParameters(connection, out var provider));

            var uri = new Uri(provider.GetBaseEndpointUri(), provider.GetRelativeCompletionUri());
            if (expectResponses)
            {
                Assert.IsType<AzureOpenAiResponsesProvider>(provider);
                Assert.Equal("https://x.openai.azure.com/openai/v1/responses", uri.ToString());
            }
            else
            {
                Assert.IsType<AzureOpenAiChatCompletionProvider>(provider);
                Assert.Equal("https://x.openai.azure.com/openai/deployments/my-deployment/chat/completions?api-version=2024-10-21", uri.ToString());
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Response_Blocked_Throws(bool flaggedCategory)
        {
            var filters = flaggedCategory ? ResponsesWire.AzureContentFilters(false, true) : ResponsesWire.AzureContentFiltersBlockedWithoutCategory();
            var body = ResponsesWire.ResponseWithExtra("completed", filters, ResponsesWire.TextMessage("hi"));

            await WithAzureClient(ProviderSettings(), _ => Ok(body), async (client, ctx) =>
            {
                var usage = new AiUsage();
                var ex = await Assert.ThrowsAsync<RefusedToAnswerException>(() =>
                    client.CompleteAsync(ctx, new AiChatRequest { Messages = [Msg(ctx, "user", "hi")] }, usage, null, CancellationToken.None));

                Assert.Equal(flaggedCategory ? "completion: " + Blocked : "completion: Response blocked due to content policy", ex.Refusal);
                Assert.Equal(ResponsesWire.InputTokens, usage.PromptTokens);
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData("completed")]
        [InlineData("incomplete")]
        [InlineData("created")]
        public async Task Streaming_Blocked_Throws(string blockedIn)
        {
            var sse = blockedIn switch
            {
                "completed" => ResponsesWire.Stream(ResponsesWire.TextDelta("hi"), ResponsesWire.CompletedEvent(ResponsesWire.AzureContentFilters(false, true))),
                "incomplete" => ResponsesWire.Stream(ResponsesWire.TextDelta("hi"), ResponsesWire.IncompleteEvent("content_filter", ResponsesWire.AzureContentFilters(false, true))),
                _ => ResponsesWire.Stream(
                    ResponsesWire.CreatedEvent(ResponsesWire.AzureContentFilters(true, false)),
                    ResponsesWire.TextDelta("hi"),
                    ResponsesWire.CompletedEvent(ResponsesWire.AzureContentFilters(false, false)))
            };

            await WithAzureClient(ProviderSettings(), _ => Sse(sse), async (client, ctx) =>
            {
                var usage = new AiUsage();
                var ex = await Assert.ThrowsAsync<RefusedToAnswerException>(() => StreamAsync(client, ctx, usage));

                Assert.Equal(blockedIn == "created" ? "prompt: " + Blocked : "completion: " + Blocked, ex.Refusal);
                if (blockedIn != "created") // the created event carries no usage
                    Assert.Equal(ResponsesWire.InputTokens, usage.PromptTokens);
            });
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [InlineData(404, """{"error":{"type":"invalid_request_error","code":"DeploymentNotFound","message":"The API deployment for this resource does not exist."}}""", ErrorType.Unknown)]
        [InlineData(429, """{"error":{"code":"RateLimitReached","message":"Your token rate limit was exceeded."}}""", ErrorType.TooManyTokens)]
        [InlineData(429, """{"error":{"code":"insufficient_quota","message":"quota"}}""", ErrorType.InsufficientQuota)]
        [InlineData(400, """{"error":{"code":"content_filter","message":"filtered"}}""", ErrorType.RefusedToAnswer)]
        public void Errors_MapAsOnAzureChatCompletions(int status, string body, ErrorType expected)
        {
            using var context = JsonOperationContext.ShortTermSingleUse();
            using var content = context.Sync.ReadForMemory(body, "error");
            using var httpResponse = new HttpResponseMessage((HttpStatusCode)status);

            var responses = new AzureOpenAiResponsesProvider(ProviderSettings()).ParseError(content, httpResponse);
            var chatCompletions = new AzureOpenAiChatCompletionProvider(ProviderSettings()).ParseError(content, httpResponse);

            Assert.Equal(expected, responses.ErrorType);
            Assert.Equal(chatCompletions.ErrorType, responses.ErrorType);
        }

        private static AzureOpenAiSettings ProviderSettings() =>
            new("k", "https://x.openai.azure.com/", "some-model", "my-deployment");

        private static async Task WithAzureClient(AzureOpenAiSettings settings, Func<string, HttpResponseMessage> respond, Func<CapturingChatCompletionClient, JsonOperationContext, Task> body)
        {
            using var contextPool = new JsonContextPool();
            using var client = new CapturingChatCompletionClient(contextPool, new AzureOpenAiResponsesProvider(settings), respond);
            using (contextPool.AllocateOperationContext(out JsonOperationContext ctx))
                await body(client, ctx);
        }

        private static Task<AiResponse> StreamAsync(CapturingChatCompletionClient client, JsonOperationContext ctx, AiUsage usage = null) =>
            client.StreamingCompleteAsync(ctx, client.Pool, "Answer", new AiChatRequest { Messages = [Msg(ctx, "user", "hi")] },
                _ => Task.CompletedTask, usage ?? new AiUsage(), null, CancellationToken.None);

        private static BlittableJsonReaderObject Msg(JsonOperationContext ctx, string role, string content) =>
            ctx.ReadObject(new DynamicJsonValue { ["role"] = role, ["content"] = content }, "msg");
    }
}
