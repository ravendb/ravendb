using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI
{
    public class RavenDB_26109 : RavenTestBase
    {
        public RavenDB_26109(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.AzureOpenAI | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task TestConnectionEndpoint_ShouldSucceed(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);

            var result = await store.Maintenance.SendAsync(new AiConnectionTests.TestAiConnectionStringOperation(configuration.Connection));

            Assert.True(result.Error == null, result.Error);
            Assert.True(result.Success);
        }
    }
}
