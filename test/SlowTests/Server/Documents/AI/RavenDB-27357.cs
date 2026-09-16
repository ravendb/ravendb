using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Conventions;
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

namespace SlowTests.Server.Documents.AI
{
    public class RavenDB_27357 : RavenTestBase
    {
        public RavenDB_27357(ITestOutputHelper output) : base(output)
        {
        }

        private const string RefusalInsteadOfContentResponse = """
                                                               {
                                                                 "id": "chatcmpl-",
                                                                 "object": "chat.completion",
                                                                 "created": 1785734451,
                                                                 "model": "gpt-4.1-2025-04-14",
                                                                 "choices": [
                                                                   {
                                                                     "index": 0,
                                                                     "message": { "role": "assistant", "content": null, "refusal": "Hello! How can I help you today?", "annotations": [] },
                                                                     "logprobs": null,
                                                                     "finish_reason": "stop"
                                                                   }
                                                                 ],
                                                                 "usage": { "prompt_tokens": 115, "completion_tokens": 50, "total_tokens": 165 },
                                                                 "service_tier": "default",
                                                                 "system_fingerprint": ""
                                                               }
                                                               """;

        private const string BadRequestResponse = """
                                                  {
                                                    "error": { "message": "Unrecognized request argument supplied: tools", "type": "invalid_request_error", "param": null, "code": null }
                                                  }
                                                  """;

        [RavenFact(RavenTestCategory.Ai)]
        public async Task SupportsToolsProbe_ShouldNotFail_WhenModelAnswersWithRefusalInsteadOfContent()
        {
            using var contextPool = NewContextPool();
            using var client = CannedResponseClient(contextPool, HttpStatusCode.OK, RefusalInsteadOfContentResponse);

            Assert.True(await client.TestSupportsToolsAsync(default));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task AcceptsImageInputProbe_ShouldNotReportFalse_WhenModelAnswersWithRefusalInsteadOfContent()
        {
            using var contextPool = NewContextPool();
            using var client = CannedResponseClient(contextPool, HttpStatusCode.OK, RefusalInsteadOfContentResponse);

            Assert.True(await client.TestAcceptsImageInputAsync(default));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task SupportsToolsProbe_ShouldReportFalse_WhenProviderRejectsTheRequest()
        {
            using var contextPool = NewContextPool();
            using var client = CannedResponseClient(contextPool, HttpStatusCode.BadRequest, BadRequestResponse);

            Assert.False(await client.TestSupportsToolsAsync(default));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task AcceptsImageInputProbe_ShouldReportFalse_WhenProviderRejectsTheRequest()
        {
            using var contextPool = NewContextPool();
            using var client = CannedResponseClient(contextPool, HttpStatusCode.BadRequest, BadRequestResponse);

            Assert.False(await client.TestAcceptsImageInputAsync(default));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task Completion_ShouldStillThrow_WhenModelAnswersWithRefusalInsteadOfContent()
        {
            using var contextPool = NewContextPool();
            using var client = CannedResponseClient(contextPool, HttpStatusCode.OK, RefusalInsteadOfContentResponse);

            var schema = ChatCompletionClient.GetSchemaFromSampleObject("{\"answer\":\"the answer to the user's prompt\"}");

            await Assert.ThrowsAsync<RefusedToAnswerException>(() => client.TestCompleteAsync("system", "hi", schema, default));
        }

        private static TransactionContextPool NewContextPool() =>
            new(RavenLogManager.Instance.CreateNullLogger(), new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()));

        private static CannedResponseChatCompletionClient CannedResponseClient(IMemoryContextPool contextPool, HttpStatusCode statusCode, string responseJson)
        {
            var connection = new AiConnectionString
            {
                ModelType = AiModelType.Chat,
                Name = "test-connection",
                OpenAiSettings = new OpenAiSettings(apiKey: "test-key", endpoint: null, model: "gpt-4.1")
            };

            Assert.True(AbstractChatCompletionClientSettings.TryGetParameters(connection, out var settings));

            return new CannedResponseChatCompletionClient(contextPool, settings, statusCode, responseJson);
        }

        private sealed class CannedResponseChatCompletionClient(
            IMemoryContextPool contextPool,
            AbstractChatCompletionClientSettings settings,
            HttpStatusCode statusCode,
            string responseJson)
            : ChatCompletionClient(contextPool, settings, ChatCompletionClient.ConventionsToUse)
        {
            protected override Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token)
            {
                return Task.FromResult(new HttpResponseMessage(statusCode)
                {
                    Content = new StringContent(responseJson, Encoding.UTF8, "application/json")
                });
            }
        }
    }
}
