using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Numerics;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Web.System;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI
{
    public class AiConnectionTests : RavenTestBase
    {
        public AiConnectionTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [ClassData(typeof(IntegrationDataType<GenAi>))]
        public void CanConnectGenAi(RavenAiIntegration integration)
        {
            var config = RavenGenAiDataAttribute.GetAiConnectionStrings(integration).Single();
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
            {
                Assert.False(config.MissingRequiredEnvVariables(out var missing),$"Missing env variable {missing}");
                Assert.True(config.TryConnect(out _, cts.Token));
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [ClassData(typeof(IntegrationDataType<Embeddings>))]
        public void CanConnectEmbeddings(RavenAiIntegration integration)
        {
            var config = RavenAiEmbeddingsDataAttribute.GetAiConnectionStrings(integration).Single();
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
            {
                Assert.False(config.MissingRequiredEnvVariables(out var missing),$"Missing env variable {missing}");
                Assert.True(config.TryConnect(out _, cts.Token));
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.AzureOpenAI | RavenAiIntegration.OpenAi | RavenAiIntegration.vLLM | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
        public void CanTestAiChatConnectionString(Options options, GenAiConfiguration configuration)
        {
            using (var store = GetDocumentStore())
            {
                var op = new TestAiConnectionStringOperation(configuration.Connection);
                var r = store.Maintenance.Send(op);
                Assert.True(r.Error == null, r.Error);
                Assert.True(r.Success);
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenAiEmbeddingsData(IntegrationType = RavenAiIntegration.All, DatabaseMode = RavenDatabaseMode.Single)]
        public void CanTestAiEmbeddingsConnectionString(Options options, EmbeddingsGenerationConfiguration configuration)
        {
            using (var store = GetDocumentStore())
            {
                var op = new TestAiConnectionStringOperation(configuration.Connection);
                var r = store.Maintenance.Send(op);
                Assert.True(r.Error == null, r.Error);
                Assert.True(r.Success);
            }
        }

        private class IntegrationDataType<T> : IEnumerable<object[]>
        {
            public IEnumerator<object[]> GetEnumerator()
            {
                foreach (var value in Enum.GetValues<RavenAiIntegration>())
                {
                    if (value == RavenAiIntegration.None)
                        continue;

                    if (BitOperations.PopCount((uint)value) > 1)
                        continue; // skip combinations

                    if (typeof(T) == typeof(GenAi))
                    {
                        // not all GenAI integrations support chat/completions
                        switch (value)
                        {
                            case RavenAiIntegration.Onnx:
                            case RavenAiIntegration.Google:
                            case RavenAiIntegration.HuggingFace:
                            case RavenAiIntegration.MistralAi:
                            case RavenAiIntegration.Vertex:
                                continue;
                        }
                    }

                    yield return [value];
                }
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        private struct GenAi
        {
        }

        private struct Embeddings
        {
        }

        private class TestAiConnectionStringOperation : IMaintenanceOperation<NodeConnectionTestResult>
        {
            private readonly AiConnectionString _connectionString;

            public TestAiConnectionStringOperation(AiConnectionString connectionString)
            {
                _connectionString = connectionString;
            }

            public RavenCommand<NodeConnectionTestResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new TestAiConnectionStringCommand(_connectionString);
            }

            private class TestAiConnectionStringCommand : RavenCommand<NodeConnectionTestResult>
            {
                private readonly AiConnectionString _connectionString;

                public TestAiConnectionStringCommand(AiConnectionString connectionString)
                {
                    _connectionString = connectionString;
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/admin/ai/test-connection?type={_connectionString.GetActiveProvider()}&modelType={_connectionString.ModelType}";
                    return new HttpRequestMessage
                    {
                        RequestUri = new Uri(url),
                        Method = HttpMethod.Post,
                        Content = new BlittableJsonContent(async stream =>
                        {
                            await ctx.WriteAsync(stream, ctx.ReadObject(ctx.ReadObject(_connectionString.GetActiveProviderInstance().ToJson(), "connection"), "connection"));
                        }, DocumentConventions.Default)
                    };
                }


                private static Func<BlittableJsonReaderObject, NodeConnectionTestResult> NodeConnectionTestResult = JsonDeserializationBase.GenerateJsonDeserializationRoutine<NodeConnectionTestResult>();

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        throw new InvalidOperationException("Response is null");

                    Result = NodeConnectionTestResult(response);
                }
            }
        }
    }
}
