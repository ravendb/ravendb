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

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_24900 : ClusterTestBase
    {
        public RavenDB_24900(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task WillThrowIfUnexpectedActionCalled(Options options, GenAiConfiguration config)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = CreateShoppingAssistant(config.ConnectionStringName);

            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/",
                new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("what kind of milk do you have? Call the ProductSearch tool to figure it out");

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
                Assert.Null(result.Answer); // should never be called, but here to show what answer is raised
            });
            Assert.Contains("There is no action defined for action 'ProductSearch' on agent 'shopping-assistant'", ex.Message);
        }
        
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task EventWillBeRaisedForUnexpectedActions(Options options, GenAiConfiguration config)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = CreateShoppingAssistant(config.ConnectionStringName);

            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/",
                new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            string action = null;
            chat.OnUnhandledAction += args =>
            {
                action = args.Action.Name;
                return Task.CompletedTask;
            };

            chat.SetUserPrompt("Call ProductSearch tool to figure out what kind of milk do you have?");

            var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            if ((AiConversationResult.ActionRequired == result.Status) == false)
            {
                Output.WriteLine(result.Answer.Answer);
            }
            Assert.Equal(AiConversationResult.ActionRequired, result.Status);
            Assert.Equal("ProductSearch", action);
        }

        private class ProductSearchArgs
        {
            public string[] Query { get; set; }
        }
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanRegisterToReceiveActions(Options options, GenAiConfiguration config)
        {
            using var store = await GetClusterStoreAsync(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = CreateShoppingAssistant(config.ConnectionStringName);

            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/",
                new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            bool called = false;
            chat.Receive<ProductSearchArgs>("ProductSearch", (req, args) =>
            {
                called = true;
            });
            

            chat.SetUserPrompt("Call ProductSearch tool to figure out what kind of milk do you have?");

            var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.ActionRequired, result.Status);
            Assert.True(called);
        }
        
        private static AiAgentConfiguration CreateShoppingAssistant(string connectionStringName)
        {
            return new AiAgentConfiguration("shopping assistant", connectionStringName,
                "You are an AI agent of an online shop. " +
                "You must use the ProductSearch tool whenever the user asks about products, availability, or catalog items. " +
                "Do not answer from your own knowledge. " +
                "Always call the ProductSearch tool first and base your response only on its results. " +
                "When talking about orders or products, include the ids as well.")
            {
                SampleObject = "{\"Answer\": \"The answer for the user's question\"}", 
                Actions = [
                    new AiAgentToolAction { 
                        Name = "ProductSearch", 
                        Description = "semantic search the store product catalog", 
                        ParametersSampleObject = "{\"Query\": [\"query terms to search for\"] }" }
                    ,
                ],
                Parameters =
                [
                    new AiAgentParameter("company")
                ]
            };
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
