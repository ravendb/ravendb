using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_25962 : RavenTestBase
    {
        public RavenDB_25962(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanDeleteAiAgent(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("test-agent", config.ConnectionStringName,
                "You are a test agent.");
            agent.Identifier = "test-agent";

            var createResult = await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance);

            // verify agent was created
            var agentFromServer = await store.AI.GetAgentAsync(createResult.Identifier);
            Assert.NotNull(agentFromServer);

            // delete agent
            var deleteResult = await store.AI.DeleteAgentAsync(createResult.Identifier);
            Assert.NotNull(deleteResult);

            // verify agent is gone
            var agents = await store.AI.GetAgentsAsync();
            Assert.Empty(agents.AiAgents);
        }
    }
}
