using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_27219(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenFact(RavenTestCategory.Ai)]
        public async Task ToolsProbe_ProviderSupportsTools_ReturnsTrue()
        {
            using var harness = CreateMockClient(MockResponseBehavior.Success);
            Assert.True(await harness.Client.TestSupportsToolsAsync(CancellationToken.None));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task ToolsProbe_ProviderRejectsToolsWith400_ReturnsFalse()
        {
            using var harness = CreateMockClient(MockResponseBehavior.RejectsTools);
            Assert.False(await harness.Client.TestSupportsToolsAsync(CancellationToken.None));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task ToolsProbe_RateLimited_Propagates()
        {
            using var harness = CreateMockClient(MockResponseBehavior.RateLimited);
            await Assert.ThrowsAnyAsync<TooManyRequestsException>(() => harness.Client.TestSupportsToolsAsync(CancellationToken.None));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task ToolsProbe_ServerError_Propagates()
        {
            using var harness = CreateMockClient(MockResponseBehavior.ServerError);
            var ex = await Assert.ThrowsAnyAsync<UnsuccessfulAiRequestException>(() => harness.Client.TestSupportsToolsAsync(CancellationToken.None));
            Assert.Equal(HttpStatusCode.InternalServerError, ex.StatusCode);
        }

        private enum MockResponseBehavior
        {
            Success,
            RejectsTools,
            RateLimited,
            ServerError,
        }

        private sealed class MockLlm : ChatCompletionClient
        {
            private readonly MockResponseBehavior _behavior;

            internal MockLlm(IMemoryContextPool contextPool, AbstractChatCompletionClientSettings settings, DocumentConventions conventions, MockResponseBehavior behavior)
                : base(contextPool, settings, conventions)
            {
                _behavior = behavior;
            }

            protected override Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token)
            {
                var response = _behavior switch
                {
                    MockResponseBehavior.Success => JsonResponse(HttpStatusCode.OK, OpenAiStyleSuccessResponse),

                    MockResponseBehavior.RejectsTools => JsonResponse(HttpStatusCode.BadRequest,
                        "{\"error\":{\"message\":\"This model does not support tools.\",\"type\":\"invalid_request_error\",\"param\":\"tools\",\"code\":null}}"),

                    MockResponseBehavior.RateLimited => JsonResponse(HttpStatusCode.TooManyRequests,
                        "{\"error\":{\"message\":\"Rate limit reached for requests.\",\"type\":\"requests\",\"param\":null,\"code\":\"rate_limit_exceeded\"}}"),

                    MockResponseBehavior.ServerError => JsonResponse(HttpStatusCode.InternalServerError,
                        "{\"error\":{\"message\":\"The server had an error while processing your request.\",\"type\":\"server_error\",\"param\":null,\"code\":null}}"),

                    _ => throw new InvalidOperationException($"Unknown behavior: {_behavior}")
                };

                return Task.FromResult(response);

                static HttpResponseMessage JsonResponse(HttpStatusCode statusCode, string body) => new(statusCode)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json")
                };
            }
        }

        private const string OpenAiStyleSuccessResponse =
            "{\"id\":\"chatcmpl-mock\",\"object\":\"chat.completion\",\"created\":1754549498,\"model\":\"gpt-4.1-mini\"," +
            "\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"hello\",\"refusal\":null,\"annotations\":[]}," +
            "\"logprobs\":null,\"finish_reason\":\"stop\"}]," +
            "\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15," +
            "\"prompt_tokens_details\":{\"cached_tokens\":0,\"audio_tokens\":0}," +
            "\"completion_tokens_details\":{\"reasoning_tokens\":0,\"audio_tokens\":0,\"accepted_prediction_tokens\":0,\"rejected_prediction_tokens\":0}}," +
            "\"service_tier\":\"default\",\"system_fingerprint\":\"fp_mock\"}";

        private static MockClientHarness CreateMockClient(MockResponseBehavior behavior)
        {
            var connection = new AiConnectionString
            {
                Name = "mock-ai-connection",
                ModelType = AiModelType.Chat,
                OpenAiSettings = new OpenAiSettings(apiKey: "sk-test-dummy", endpoint: "https://api.openai.com/", model: "gpt-4.1-mini")
            };

            Assert.True(AbstractChatCompletionClientSettings.TryGetParameters(connection, out var settings));

            StorageEnvironment storageEnv = null;
            TransactionContextPool contextPool = null;
            MockLlm client = null;
            try
            {
                storageEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests());
                contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), storageEnv);
                client = new MockLlm(contextPool, settings, ChatCompletionClient.ConventionsToUse, behavior);
                return new MockClientHarness(storageEnv, contextPool, client);
            }
            catch
            {
                client?.Dispose();
                contextPool?.Dispose();
                storageEnv?.Dispose();
                throw;
            }
        }

        private sealed class MockClientHarness(StorageEnvironment storageEnv, TransactionContextPool contextPool, MockLlm client) : IDisposable
        {
            public MockLlm Client => client;

            public void Dispose()
            {
                client.Dispose();
                contextPool.Dispose();
                storageEnv.Dispose();
            }
        }
    }
}
