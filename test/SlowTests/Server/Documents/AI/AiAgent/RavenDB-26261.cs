using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_26261(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const string ConnectionStringName = "mock-ai-connection";

        [RavenFact(RavenTestCategory.Ai)]
        public async Task AzureOpenAiStyleTextPlainResponse_SurfacesCleanError()
        {
            using var store = GetDocumentStore();
            await PutDummyConnectionStringAsync(store);

            var agent = BuildAgentConfig();

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var handler = new MockHandler(Server.ServerStore, database, MockResponseBehavior.AzureGatewayTextPlain)
                {
                    Authentication = null
                };
                handler.Initialize(agent, "Dummy", new RequestBody
                {
                    Parameters = null,
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "describe the attached PDF"
                }, changeVector: null);

                var ex = await Assert.ThrowsAnyAsync<Exception>(
                    () => handler.HandleRequestAsync(context, CancellationToken.None));

                var full = ex.ToString();

                Assert.DoesNotContain("InvalidDataException", full);
                Assert.DoesNotContain("Cannot have a", full);

                Assert.Contains("text/plain", full);
                Assert.Contains("Bad Request", full);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task TestConnection_AcceptsText_RejectsImages_ProbeReturnsFalse()
        {
            var connection = new AiConnectionString
            {
                Name = ConnectionStringName,
                ModelType = AiModelType.Chat,
                OpenAiSettings = new OpenAiSettings(apiKey: "sk-test-dummy", endpoint: "https://api.openai.com/", model: "gpt-4.1-mini")
            };

            Assert.True(AbstractChatCompletionClientSettings.TryGetParameters(connection, out var settings));

            using var storageEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests());
            using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), storageEnv);
            using var client = new MockLlm(contextPool, settings, ChatCompletionClient.ConventionsToUse, MockResponseBehavior.AcceptsTextRejectsImages);

            var schema = ChatCompletionClient.GetSchemaFromSampleObject("{}");
            await client.TestCompleteAsync("Reply with exact word only: raven", "hi", schema, CancellationToken.None);

            var acceptsImageInput = await client.TestAcceptsImageInputAsync(CancellationToken.None);
            Assert.False(acceptsImageInput);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task TestConnection_AcceptsTextAndImages_ProbeReturnsTrue()
        {
            var connection = new AiConnectionString
            {
                Name = ConnectionStringName,
                ModelType = AiModelType.Chat,
                OpenAiSettings = new OpenAiSettings(apiKey: "sk-test-dummy", endpoint: "https://api.openai.com/", model: "gpt-4.1-mini")
            };

            Assert.True(AbstractChatCompletionClientSettings.TryGetParameters(connection, out var settings));

            using var storageEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests());
            using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), storageEnv);
            using var client = new MockLlm(contextPool, settings, ChatCompletionClient.ConventionsToUse, MockResponseBehavior.OpenAiSuccess);

            var schema = ChatCompletionClient.GetSchemaFromSampleObject("{}");
            await client.TestCompleteAsync("Reply with exact word only: raven", "hi", schema, CancellationToken.None);

            var acceptsImageInput = await client.TestAcceptsImageInputAsync(CancellationToken.None);
            Assert.True(acceptsImageInput);

            // The probe must actually send the image, exactly once (guards against a dropped image or a re-introduced double-send).
            Assert.Equal(1, Regex.Matches(client.LastRequestBody, "data:image/png;base64,").Count);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.AzureOpenAI | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
        public void TestConnection_RealProvider_ReportsAcceptsImageInputTrue(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            var op = new AiConnectionTests.TestAiConnectionStringOperation(config.Connection);
            var r = store.Maintenance.Send(op);

            Assert.True(r.Error == null, r.Error);
            Assert.True(r.Success);
            Assert.True(r.AcceptsImageInput);
        }

        private static AiAgentConfiguration BuildAgentConfig()
        {
            var agent = new AiAgentConfiguration("pdf-analyzer", ConnectionStringName,
                "You are a helpful assistant. Describe attached files.")
            {
                SampleObject = "{\"Answer\":\"description of the PDF\"}"
            };
            return agent;
        }

        private static async Task PutDummyConnectionStringAsync(Raven.Client.Documents.IDocumentStore store)
        {
            var connection = new AiConnectionString
            {
                Name = ConnectionStringName,
                ModelType = AiModelType.Chat,
                OpenAiSettings = new OpenAiSettings(apiKey: "sk-test-dummy", endpoint: "https://api.openai.com/", model: "gpt-4.1-mini")
            };

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(connection));
        }

        private enum MockResponseBehavior
        {
            OpenAiSuccess,
            AzureGatewayTextPlain,
            AcceptsTextRejectsImages,
        }

        private sealed class MockHandler : ConversationHandler
        {
            private readonly DocumentDatabase _database;
            private readonly MockResponseBehavior _behavior;

            public MockHandler(Raven.Server.ServerWide.ServerStore server, DocumentDatabase database, MockResponseBehavior behavior)
                : base(server, database)
            {
                _database = database;
                _behavior = behavior;
            }

            protected internal override ChatCompletionClient CreateClient()
            {
                var connection = GetAiConnectionString();
                if (AbstractChatCompletionClientSettings.TryGetParameters(connection, out var settings) == false)
                    throw new NotSupportedException($"The specified provider (\"{connection.GetActiveProvider()}\") is not supported.");

                return new MockLlm(_database.DocumentsStorage.ContextPool, settings, ChatCompletionClient.ConventionsToUse, _behavior);
            }
        }

        private sealed class MockLlm : ChatCompletionClient
        {
            private readonly MockResponseBehavior _behavior;

            internal MockLlm(IMemoryContextPool contextPool, AbstractChatCompletionClientSettings settings, DocumentConventions conventions, MockResponseBehavior behavior)
                : base(contextPool, settings, conventions)
            {
                _behavior = behavior;
            }

            public string LastRequestBody;

            protected override async Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token)
            {
                LastRequestBody = request.Content != null ? await request.Content.ReadAsStringAsync(token) : null;

                return _behavior switch
                {
                    MockResponseBehavior.OpenAiSuccess => OpenAiSuccess(),

                    MockResponseBehavior.AzureGatewayTextPlain => AzureGatewayBadRequest(),

                    MockResponseBehavior.AcceptsTextRejectsImages => RequestContainsImagePngDataUrl(LastRequestBody)
                        ? AzureGatewayBadRequest()
                        : OpenAiSuccess(),

                    _ => throw new InvalidOperationException($"Unknown behavior: {_behavior}")
                };

                static HttpResponseMessage OpenAiSuccess() => new(HttpStatusCode.OK)
                {
                    Content = new StringContent(OpenAiStyleSuccessResponse, Encoding.UTF8, "application/json")
                };

                static HttpResponseMessage AzureGatewayBadRequest() => new(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent("Bad Request", Encoding.UTF8, "text/plain")
                };

                static bool RequestContainsImagePngDataUrl(string body) => body.Contains("data:image/png;base64,");
            }
        }

        private const string OpenAiStyleSuccessResponse =
            "{\"id\":\"chatcmpl-mock\",\"object\":\"chat.completion\",\"created\":1754549498,\"model\":\"gpt-4.1-mini\"," +
            "\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"{\\\"Answer\\\":\\\"PDF received\\\"}\",\"refusal\":null,\"annotations\":[]}," +
            "\"logprobs\":null,\"finish_reason\":\"stop\"}]," +
            "\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15," +
            "\"prompt_tokens_details\":{\"cached_tokens\":0,\"audio_tokens\":0}," +
            "\"completion_tokens_details\":{\"reasoning_tokens\":0,\"audio_tokens\":0,\"accepted_prediction_tokens\":0,\"rejected_prediction_tokens\":0}}," +
            "\"service_tier\":\"default\",\"system_fingerprint\":\"fp_mock\"}";
    }
}
