using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Tests.Infrastructure.Commands;
using Xunit;
#pragma warning disable SKEXP0001

namespace SlowTests.Issues
{
    public class RavenDB_24505 : RavenTestBase
    {
        public RavenDB_24505(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanCreateOpenAiEmbeddingConnectionStringAndTestGenAiConnection1(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);

            configuration.Connection.ModelType = AiModelType.TextEmbeddings;
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));

            var database = await GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testGenAiScript = new TestGenAiScript { Configuration = configuration };
                var bjro = store.Conventions.Serialization.DefaultConverter.ToBlittable(testGenAiScript, context);
                var cmd = new GenAiTestCommand(DocumentConventions.DefaultForServer, bjro);

                using var requestExecutor = store.GetRequestExecutor();
                var error = await Assert.ThrowsAsync<RavenException>(async () => await requestExecutor.ExecuteAsync(cmd, context));

                Assert.IsType<InvalidOperationException>(error.InnerException);
                Assert.Contains("ModelType of GenAI configuration must be Chat", error.InnerException.Message);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenAiEmbeddingsData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task EmbeddingsGeneration_ShouldReject_ChatModelType(
            Options options,
            EmbeddingsGenerationConfiguration cfg)
        {
            using var store = GetDocumentStore(options);

            cfg.Connection.ModelType = AiModelType.Chat;
            
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(cfg.Connection));

            var cmd = new AddEmbeddingsGenerationOperation(cfg);
            
            var ex = await Assert.ThrowsAsync<RavenException>(() =>
                store.Maintenance.SendAsync(cmd));

            Assert.IsType<InvalidOperationException>(ex.InnerException);
            Assert.Contains("ModelType of Embeddings Generation configuration must be TextEmbeddings",
                ex.InnerException.Message);
        } 
    }
}


