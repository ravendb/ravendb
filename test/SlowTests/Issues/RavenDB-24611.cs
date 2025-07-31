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
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
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

            var agentId = (await store.AI.CreateAgentAsync<OutputSchema>(agent)).Identifier;

            var chat = store.AI.StartConversation<OutputSchema>(agentId, p => p.AddParameter("Company", "companies/90-A"));

            chat.SetUserPrompt("what are my recent orders?");

            var ex = await Assert.ThrowsAsync<RavenException>(() => chat.RunAsync(CancellationToken.None));
            Assert.Contains("AiAgentParameter' is missing", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task CanPassNestedObjectAsActionResponse(Options options, GenAiConfiguration config)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("chef assistant", config.ConnectionStringName,
                "You are a chef that know to recommend customers what to eat.");

            agent.Persistence = new AiAgentPersistenceConfiguration("Chats/", TimeSpan.FromDays(30));

            agent.Actions =
            [
                new AiAgentToolAction { Name = "RecentOrder", Description = "Get the recent orders of the current user", ParametersSampleObject = "{}" }
            ];

            var agentId = (await store.AI.CreateAgentAsync<ChefOutputSchema>(agent)).Identifier;

            var chat = store.AI.StartConversation<ChefOutputSchema>(agentId, builder: null);
            chat.SetUserPrompt("recommend me what to eat for launch, based on my recent orders");
            var result = await chat.RunAsync(CancellationToken.None);

            if (result == AiConversationResult.ActionRequired)
            {
                
                foreach (var request in chat.RequiredActions())
                {
                    chat.AddActionResponse(request.ToolId,
                    new DynamicJsonValue
                    {
                        ["RecentOrders"] = new[]{ "pizza margarita", "pizza with olives" },
                        ["customerName"] = "Golan",
                        ["Orders"] = new[]
                        {
                            new DynamicJsonValue { ["Restaurant"] = new DynamicJsonValue { ["Name"] = "Pizza Hat" }, ["Food"] = "pizza margarita" },
                            new DynamicJsonValue { ["Restaurant"] = new DynamicJsonValue { ["Name"] = "Domino's Pizza" }, ["Food"] = "pizza with olives" }
                        }
                    });
                }
                result = await chat.RunAsync(CancellationToken.None);
            }

            Assert.Equal(AiConversationResult.Done, result);
            Assert.NotNull(chat.Answer);
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

            var chat = store.AI.StartConversation<OutputSchema>(docId, builder: null);
            chat.SetUserPrompt("hello");
            await Assert.ThrowsAsync<RavenException>(() => chat.RunAsync(CancellationToken.None));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task Concurrency_When_Resuming_Same_Conversation(Options options, GenAiConfiguration config)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = CreateShoppingAssistant(config.ConnectionStringName);
            var agentId = (await store.AI.CreateAgentAsync<OutputSchema>(agent)).Identifier;

            var starter = store.AI.StartConversation<OutputSchema>(agentId, p => p.AddParameter("company", "companies/90-A"));
            starter.SetUserPrompt("first");
            var r = await starter.RunAsync(CancellationToken.None);
            var convId = starter.Id;
            Assert.Equal(AiConversationResult.Done, r);

            IAiConversationOperations<OutputSchema> firstChat = null, secondChat = null;

            async Task<AiConversationResult> FirstResumeAsync()
            {
                firstChat = store.AI.ResumeConversation<OutputSchema>(convId, starter.ChangeVector);
                firstChat.SetUserPrompt($"again");
                return await firstChat.RunAsync(CancellationToken.None);
            }

            async Task<AiConversationResult> SecondResumeAsync()
            {
                secondChat = store.AI.ResumeConversation<OutputSchema>(convId, starter.ChangeVector);
                secondChat.SetUserPrompt($"again");
                return await secondChat.RunAsync(CancellationToken.None);
            }

            var t1 = FirstResumeAsync();
            var t2 = SecondResumeAsync();

            try
            {
                await Task.WhenAll(t1, t2);
                Assert.True(false, "ConcurrencyException was expected but both operations finished successfully.");
            }
            catch (Exception ex)
            {
                var inner = ex is AggregateException ae ? ae.Flatten().InnerExceptions.First() : ex;
                Assert.IsType<ConcurrencyException>(inner);
                var failedChat = t1.IsFaulted ? firstChat : secondChat;
                failedChat.SetUserPrompt("Retry after collision");
                var retryResult = await failedChat.RunAsync(CancellationToken.None);
                Assert.Equal(AiConversationResult.Done, retryResult);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task ConcurrentActionResponsesShouldConflict(Options options, GenAiConfiguration cfg)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(
                new PutConnectionStringOperation<AiConnectionString>(cfg.Connection));

            var agent = CreateShoppingAssistant(cfg.ConnectionStringName,
                withActions: true);
            var agentId = (await store.AI.CreateAgentAsync<OutputSchema>(agent)).Identifier;

            var starter = store.AI.StartConversation<OutputSchema>(agentId, builder: null);
            starter.SetUserPrompt(
                "Please use the ProductSearch tool to find the top 5 products matching “Italian cheese” in our catalog, and also call the RecentOrder tool to fetch my last 10 orders and answer the question");
            var r = await starter.RunAsync(CancellationToken.None);
            Assert.Equal(AiConversationResult.ActionRequired, r);

            var actions = starter.RequiredActions().ToList();
            Assert.Equal(2, actions.Count);

            IAiConversationOperations<OutputSchema> firstChat = null, secondChat = null;

            async Task<AiConversationResult> FirstRespondAsync(AiAgentActionRequest action, object payload)
            {
                firstChat = store.AI.ResumeConversation<OutputSchema>(starter.Id, starter.ChangeVector);
                firstChat.AddActionResponse(action.ToolId, payload);
                return await firstChat.RunAsync(CancellationToken.None);
            }

            async Task<AiConversationResult> SecondRespondAsync(AiAgentActionRequest action, object payload)
            {
                secondChat = store.AI.ResumeConversation<OutputSchema>(starter.Id, starter.ChangeVector);
                secondChat.AddActionResponse(action.ToolId, payload);
                return await secondChat.RunAsync(CancellationToken.None);
            }

            var t1 = FirstRespondAsync(actions[0], new { });
            var t2 = SecondRespondAsync(actions[1], new { });

            try
            {
                await Task.WhenAll(t1, t2);
                Assert.True(false, "At least one ConcurrencyException expected.");
            }
            catch (Exception ex)
            {
                var inner = ex is AggregateException ae ? ae.Flatten().InnerExceptions.First() : ex;
                Assert.IsType<ConcurrencyException>(inner);
                AiAgentActionRequest action = null;
                IAiConversationOperations<OutputSchema> failedChat = null;
                if (t1.IsFaulted)
                {
                    failedChat = firstChat;
                    action = actions[0];
                }
                else
                {
                    failedChat = secondChat;
                    action = actions[1];
                }

                failedChat.AddActionResponse(action.ToolId, new { });
                var retryResult = await failedChat.RunAsync(CancellationToken.None);
                Assert.Equal(AiConversationResult.Done, retryResult);
            }
        }

        private static AiAgentConfiguration CreateShoppingAssistant(string connectionStringName, bool withActions = false)
        {
            var agent = new AiAgentConfiguration("shopping assistant", connectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            agent.Persistence = new AiAgentPersistenceConfiguration("Chats/", TimeSpan.FromDays(30));

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
            public string Answer = "the answer to the user question";
            public bool Relevant = true;
            public List<string> RelevantOrdersId = ["what are the relevant orders?"];
        }
        public class ChefOutputSchema
        {
            public string Answer = "what should the customer eat";
            public List<string> PreviousMeals = ["list of previous meals"];
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
