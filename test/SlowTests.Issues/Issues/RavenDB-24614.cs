using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_24614 : RavenTestBase
    {
        public RavenDB_24614(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.All)]
        public async Task PutAiConnectionString_WithInvalidIdentifier_ShouldThrowException(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            configuration.Connection.Identifier = "_Invalid";

            var op = new PutConnectionStringOperation<AiConnectionString>(configuration.Connection);
            var ex = await Assert.ThrowsAsync<RavenException>(async () =>
                await store.Maintenance.SendAsync(op)
            );

            Assert.IsType<InvalidOperationException>(ex.InnerException);
            Assert.Contains("Identifier contains invalid characters:", ex.InnerException.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task AddGenAiEtl_WithInvalidIdentifier_ShouldThrowException(
            Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);

            configuration.Identifier = "_Invalid";

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var op = new AddGenAiOperation(configuration);
            var ex = await Assert.ThrowsAsync<RavenException>(async () =>
                await store.Maintenance.SendAsync(op)
            );

            Assert.IsType<InvalidOperationException>(ex.InnerException);
            Assert.Contains("Identifier contains invalid characters:", ex.InnerException.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenAiEmbeddingsData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.All)]
        public async Task AddEmbeddingEtl_WithInvalidIdentifier_ShouldThrowException(Options options, EmbeddingsGenerationConfiguration configuration)
        {
            using var store = GetDocumentStore(options);

            configuration.Identifier = "_Invalid";

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var op = new AddEmbeddingsGenerationOperation(configuration);
            var ex = await Assert.ThrowsAsync<RavenException>(async () =>
                await store.Maintenance.SendAsync(op)
            );

            Assert.IsType<InvalidOperationException>(ex.InnerException);
            Assert.Contains("Identifier contains invalid characters:", ex.InnerException.Message);
        }
    }
}
