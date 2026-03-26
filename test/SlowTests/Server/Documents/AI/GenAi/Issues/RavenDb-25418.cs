using System;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDb_25418(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanTestContextGenerationWhenPromptAndSchemaAreMissing(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Prompt = null;
            config.JsonSchema = null;
            config.SampleObject = null;
            config.UpdateScript = null;
            config.Collection = "Posts";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = "ai.genContext({ Name: this.Name });"
            };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new { Name = "Danielle" }, "users/1");
                await session.SaveChangesAsync();
            }

            DocumentDatabase database = await GetDocumentDatabaseInstanceFor(store);
            using IDisposable _ = database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

            var operation = new TestCreateGenAiContextOperation("users/1", config);
            GenAiTestScriptResult result = await store.Maintenance.SendAsync(context, operation);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Results);

            var contextOutput = result.Results[0].ContextOutput.Context;
            Assert.NotNull(contextOutput);
            Assert.True(contextOutput.TryGet("Name", out string name));
            Assert.Equal("Danielle", name);
        }

        private class TestCreateGenAiContextOperation : IMaintenanceOperation<GenAiTestScriptResult>
        {
            private readonly TestGenAiScript _testGenAiScript;

            public TestCreateGenAiContextOperation(string docId, GenAiConfiguration config)
            {
                _testGenAiScript = new TestGenAiScript
                {
                    DocumentId = docId,
                    Configuration = config,
                    TestStage = TestStage.CreateContextObjects
                };
            }

            public RavenCommand<GenAiTestScriptResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new TestGenAiCommand(_testGenAiScript, conventions);
            }
        }

        private class TestGenAiCommand(TestGenAiScript testGenAiScript, DocumentConventions conventions) : RavenCommand<GenAiTestScriptResult>
        {
            public override bool IsReadRequest { get; } = true;
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/ai/gen-ai/test";
                BlittableJsonReaderObject bjro = ctx.ReadObject(testGenAiScript.ToJson(), "TestGenAiCommand_payload");
                return new HttpRequestMessage
                {
                    Method = HttpMethods.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, bjro).ConfigureAwait(false), conventions)
                };
            }
            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = DeserializeToTestEtlScriptResult(response);
            }

            private static readonly Func<BlittableJsonReaderObject, GenAiTestScriptResult> DeserializeToTestEtlScriptResult = JsonDeserializationClient.GenerateJsonDeserializationRoutine<GenAiTestScriptResult>();
        }
    }
}
