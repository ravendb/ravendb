using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_24458 : RavenTestBase
    {
        public RavenDB_24458(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task TwoConfigsWithSimilarIdentifierShouldThrow(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agents = GetAgents(config);
            agents[0].Name = "agent0 name";
            agents[1].Name = "agent1 name";
            agents[0].Identifier = "agent0-identifier";
            agents[1].Identifier = agents[0].Identifier;

            await store.Maintenance.SendAsync(AddOrUpdateAiAgentOperation.Create(agents[0], AiAgentBasics.OutputSchema.Instance));

            var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(AddOrUpdateAiAgentOperation.Create(agents[1], AiAgentBasics.OutputSchema.Instance)));
            Assert.Contains("Can't update AI Agent config: 'agent1 name'. The identifier 'agent0-identifier' is already used by AI Agent config 'agent0 name'",
                e.Message);

            agents[1].Identifier = "agent1-identifier";
            await store.Maintenance.SendAsync(AddOrUpdateAiAgentOperation.Create(agents[1], AiAgentBasics.OutputSchema.Instance));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task UpdateConfig(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agents = GetAgents(config);
            await store.Maintenance.SendAsync(AddOrUpdateAiAgentOperation.Create(agents[0], AiAgentBasics.OutputSchema.Instance));
            await AssertAgentInRecordAsync(store, agents[0]);

            agents[1].Identifier = agents[0].Identifier;
            agents[1].Name = agents[0].Name;
            await store.Maintenance.SendAsync(AddOrUpdateAiAgentOperation.Create(agents[1], AiAgentBasics.OutputSchema.Instance));
            await AssertAgentInRecordAsync(store, agents[1]);

            var res = await store.Maintenance.SendAsync(new DeleteAiAgentOperation(agents[0].Identifier));
            Assert.NotNull(res);

            var e = await Assert.ThrowsAsync<RavenException>(async () => await store.Maintenance.SendAsync(new GetAiAgentsOperation(agents[0].Identifier)));
            Assert.Contains($"AI Agent 'shopping-assistant' doesn't exists", e.Message);
        }

        private static async Task AssertAgentInRecordAsync(DocumentStore store, AiAgentConfiguration agent)
        {
            var converter = DocumentConventions.Default.Serialization.DefaultConverter;

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(1, record.AiAgents.Count);

            using var context = JsonOperationContext.ShortTermSingleUse();

            var c0 = converter.ToBlittable(agent, context);
            var d0 = converter.ToBlittable(record.AiAgents[0], context);
            Assert.True(c0.Equals(d0));
        }

        private static List<AiAgentConfiguration> GetAgents(GenAiConfiguration aiConfig)
        {
            var agent0 = new AiAgentConfiguration("shopping assistant", aiConfig.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
            agent0.Identifier = "shopping-assistant";
            agent0.Parameters.Add(new AiAgentParameter("company"));
            agent0.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "ProductSearch", 
                    Description = "semantic search the store product catalog", 
                    Query = "from Products where vector.search(embedding.text(Name), $query)", 
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                },
                new AiAgentToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSampleObject = "{}"
                }
            ];
            agent0.ChatTrimming = null;
            var agent1 = new AiAgentConfiguration("warehouse manager", aiConfig.ConnectionStringName, "You are an AI agent managing a warehouse.");
            agent1.Actions =
            [
                new AiAgentToolAction
                {
                    Name = "ProductSearch",
                    Description = "semantic search the store product catalog",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                },
                new AiAgentToolAction { Name = "RecentOrder", Description = "Get the recent orders of the current user", ParametersSampleObject = "{}" }
            ];
            agent1.ChatTrimming = null;
            return new List<AiAgentConfiguration>() { agent0, agent1 };
        }
    }
}
