using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_24611 : ClusterTestBase
    {
        public RavenDB_24611(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task ParametersAreKeySensitive(Options options, GenAiConfiguration config)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = CreateShoppingAssistant(config.ConnectionStringName);

            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/",new AiConversationCreationOptions().AddParameter("Company", "companies/90-A"));

            chat.SetUserPrompt("what are my recent orders?");

            var ex = await Assert.ThrowsAsync<RavenException>(() => chat.RunAsync<OutputSchema>(CancellationToken.None));
            Assert.Contains("Parameter 'company' is missing", ex.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task CanPassNestedObjectAsActionResponse(Options options, GenAiConfiguration config)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("chef assistant", config.ConnectionStringName,
                "You are a chef that know to recommend customers what to eat.");

            agent.Actions =
            [
                new AiAgentToolAction { Name = "RecentOrder", Description = "Get the recent orders of the current user", ParametersSampleObject = "{}" }
            ];

            var agentId = (await store.AI.CreateAgentAsync(agent, ChefOutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/", creationOptions: null);
            chat.SetUserPrompt("recommend me what to eat for launch, based on my recent orders and tell me where to but it from based on the restaurant I have ordered from");
            var result = await chat.RunAsync<ChefOutputSchema>(CancellationToken.None);

            if (result.Status == AiConversationResult.ActionRequired)
            {
                
                foreach (var request in chat.RequiredActions())
                {
                    chat.AddActionResponse(request.ToolId,
                    new OrderResponse
                    {
                        RecentOrders = new[]{ "pizza margarita", "pizza with olives" },
                        CustomerName = "Golan",
                        Orders = new[]
                        {
                            new Order { Restaurant = new Restaurant { Name = "Pizza Hat" }, Food = "pizza margarita" },
                            new Order { Restaurant = new Restaurant { Name = "Domino's Pizza" }, Food = "pizza with olives" }
                        }
                    });
                }
                result = await chat.RunAsync<ChefOutputSchema>(CancellationToken.None);
            }

            Assert.Equal(AiConversationResult.Done, result.Status);
            Assert.NotNull(result.Answer);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task ShouldThrowWhenConversationIdIsDocumentId(Options options, GenAiConfiguration config)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            const string docId = "orders/1-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new { Customer = "Foo" }, docId);
                await session.SaveChangesAsync();
            }

            var chat = store.AI.Conversation(docId, "chats/", creationOptions: null);
            chat.SetUserPrompt("hello");
            var ex = await Assert.ThrowsAsync<RavenException>(() => chat.RunAsync<ChefOutputSchema>(CancellationToken.None));
            Assert.IsType<ArgumentException>(ex.InnerException);
            Assert.Contains("'orders/1-A' doesn't exists", ex.InnerException?.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task Concurrency_When_Resuming_Same_Conversation(Options options, GenAiConfiguration config)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(
                new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = CreateShoppingAssistant(config.ConnectionStringName);
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var initial = store.AI.Conversation(
                agentId, "chats/",new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));
            initial.SetUserPrompt("What goes well with my cheese?");
            var done1 = await initial.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, done1.Status);

            var convId = initial.Id;
            var originalCv = initial.ChangeVector;
            Assert.NotNull(originalCv);

            var resume1 = store.AI.Conversation(agentId, convId, creationOptions: null, originalCv);
            resume1.SetUserPrompt("Can you suggest an alternative?");
            var done2 = await resume1.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, done2.Status);

            var freshCv = resume1.ChangeVector;
            Assert.NotNull(freshCv);
            Assert.NotEqual(originalCv, freshCv);

            var resume2 = store.AI.Conversation(agentId, convId, creationOptions: null, originalCv);
            resume2.SetUserPrompt("One more suggestion?");
            var ex = await Assert.ThrowsAsync<ConcurrencyException>(
                () => resume2.RunAsync<OutputSchema>(CancellationToken.None));
            Assert.NotNull(resume2.ChangeVector);

            resume2.SetUserPrompt("Retry after collision");
            var done3 = await resume2.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, done3.Status);
            Assert.NotNull(done3.Answer);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task ConcurrentActionResponsesShouldConflict(Options options, GenAiConfiguration cfg)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(
                new PutConnectionStringOperation<AiConnectionString>(cfg.Connection));

            var agent = CreateShoppingAssistant(cfg.ConnectionStringName, withActions: true);
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var starter = store.AI.Conversation(agentId, "chats/", creationOptions: null);
            starter.SetUserPrompt(
                "Please use ProductSearch to find the top 5 cheeses, " +
                "then use RecentOrder to fetch my last 10 orders and answer.");
            var firstRun = await starter.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.ActionRequired, firstRun.Status);

            var actions = starter.RequiredActions().ToList();
            Assert.Equal(2, actions.Count);

            var staleCv = starter.ChangeVector;
            Assert.NotNull(staleCv);

            var resume1 = store.AI.Conversation(agentId, starter.Id, creationOptions: null, staleCv);
            resume1.AddActionResponse(
                actions[0].ToolId,
                new { });
            var afterFirst = await resume1.RunAsync<OutputSchema>(CancellationToken.None);

            Assert.Equal(AiConversationResult.ActionRequired, afterFirst.Status);

            var cvAfterFirst = resume1.ChangeVector;
            Assert.NotEqual(staleCv, cvAfterFirst);

            var resume2 = store.AI.Conversation(agentId, starter.Id, creationOptions: null, staleCv);
            resume2.AddActionResponse(
                actions[1].ToolId,
                new { });

            await Assert.ThrowsAsync<ConcurrencyException>(
                () => resume2.RunAsync<OutputSchema>(CancellationToken.None));

            Assert.NotNull(resume2.ChangeVector);

            resume2.AddActionResponse(
                actions[1].ToolId,
                new { });
            var finalResult = await resume2.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, finalResult.Status);
            Assert.NotNull(finalResult.Answer);
        }

        private static AiAgentConfiguration CreateShoppingAssistant(string connectionStringName, bool withActions = false)
        {
            var agent = new AiAgentConfiguration("shopping assistant", connectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            if (withActions == false)
                agent.Parameters.Add(new AiAgentParameter("company"));

            if (withActions)
            {
                agent.Actions =
                [
                    new AiAgentToolAction { Name = "ProductSearch", Description = "semantic search the store product catalog", ParametersSampleObject = "{}" },
                    new AiAgentToolAction { Name = "RecentOrder", Description = "Get the recent orders of the current user", ParametersSampleObject = "{}" }
                ];
            }
            else
            {
                agent.Queries =
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
            }

            return agent;
        }


        public class Company
        {
            public string Name { get; set; }
            public Address Address { get; set; }
        }

        public class Address
        {
            public string City { get; set; }
            public string Country { get; set; }
        }

        public class OutputSchema
        {
            public static OutputSchema Instance = new OutputSchema();
            public string Answer = "the answer to the user question";
            public bool Relevant = true;
            public List<string> RelevantOrdersId = ["what are the relevant orders?"];
        }
        public class ChefOutputSchema
        {
            public static ChefOutputSchema Instance = new ChefOutputSchema();
            public string Answer = "what should the customer eat";
            public List<string> PreviousMeals = ["list of previous meals"];
        }
        public class OrderResponse
        {
            public string[] RecentOrders { get; set; }
            public string CustomerName { get; set; }
            public Order[] Orders { get; set; }
        }

        public class Order
        {
            public Restaurant Restaurant { get; set; }
            public string Food { get; set; }
        }

        public class Restaurant
        {
            public string Name { get; set; }
        }
        private async Task<Raven.Client.Documents.DocumentStore> GetClusterStoreAsync(Options originalOptions)
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            return GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3,
                Path = originalOptions?.Path, // keep any custom path from the framework
                ModifyDatabaseRecord = originalOptions?.ModifyDatabaseRecord,
            });
        }
    }
}
