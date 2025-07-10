using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentConfigurationValidation : RavenTestBase
    {
        public AiAgentConfigurationValidation(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task ThrowOnMissingAgentParameter(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            agent.Persistence = new AiAgentPersistenceConfiguration
            {
                Collection = "Chats",
                Expires = TimeSpan.FromDays(30)
            };

            agent.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "ProductSearch", 
                    Description =  "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query)",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                }
                ,
                new AiAgentToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSampleObject = "{}"
                }
            ];

            var e = await Assert.ThrowsAsync<RavenException>( () => store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<AiAgentBasics.OutputSchema>(agent)));
            Assert.Contains("Tool query 'RecentOrder' contains parameters that are not defined in the agent configuration: 'company'", e.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task ThrowOnDuplicateToolParameterUsage(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            agent.Persistence = new AiAgentPersistenceConfiguration
            {
                Collection = "Chats",
                Expires = TimeSpan.FromDays(30)
            };

            agent.Parameters.Add("company");
            agent.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "ProductSearch", 
                    Description =  "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $company)",
                    ParametersSampleObject = "{\"company\": [\"term or phrase to search in the catalog\"]}"
                }
                ,
                new AiAgentToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSampleObject = "{}"
                }
            ];

            var e = await Assert.ThrowsAsync<RavenException>( () => store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<AiAgentBasics.OutputSchema>(agent)));
            Assert.Contains("Parameter company is defined on both the agent level and the query level for ProductSearch", e.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task ThrowOnMissingChatParameter(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            agent.Persistence = new AiAgentPersistenceConfiguration
            {
                Collection = "Chats",
                Expires = TimeSpan.FromDays(30)
            };
            
            agent.Parameters.Add("company");
            agent.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "ProductSearch", 
                    Description =  "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query)",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                }
                ,
                new AiAgentToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSampleObject = "{}"
                }
            ];

            var identifier = (await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<AiAgentBasics.OutputSchema>(agent))).Identifier;
            var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new RunConversationOperation<AiAgentBasics.OutputSchema>(identifier, "what goes well with my cheese for recent orders?", parameters: null)));
            Assert.Contains($"Parameter 'company' is missing", e.Message);
        }
    }
}
