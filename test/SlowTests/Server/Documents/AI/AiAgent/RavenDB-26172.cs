using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_26172(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task AgentParameterTypeTest(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            await SeedOrders(store);

            // products-search-agent
            var ordersAgent = new AiAgentConfiguration(
                "orders-search-agent",
                config.ConnectionStringName,
                "Agent responsible for retrieving orders from the database."
            )
            {
                Queries = new List<AiAgentToolQuery>
                {
                    new AiAgentToolQuery
                    {
                        Name = "GetOrders",
                        Description = "Return orders. use it when you asked for orders.",
                        Query = "from Orders " +
                                "where OrderDate > $date",
                        ParametersSampleObject = "{}"
                    }
                }
            };
            ordersAgent.Parameters.Add(new AiAgentParameter("date", "date", sendToModel: false) { Type = AiAgentParameterValueType.String });
            var ordersAgentId = (await store.AI.CreateAgentAsync(ordersAgent, OutputSchema.Instance)).Identifier;
            
            var chat = store.AI.Conversation(
                ordersAgentId,
                "Chats/1",
                new AiConversationCreationOptions().AddParameter("date", new DateTime(2026, 3, 7))
            );
            
            chat.SetUserPrompt(
                "Show me the orders."
            );
            var r = await chat.RunAsync<OutputSchema>();

            using var session = store.OpenSession();
            var results = session.Advanced
                .RawQuery<Order>(@"
        from Orders
        where OrderDate > $date
    ")
                .AddParameter("date", new DateTime(2026, 3, 7)).ToList();

            Assert.Equal(results.Count, r.Answer.OrdersIds.Count);
        }

        private static async Task SeedOrders(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Customer = "Alice",
                    OrderDate = new DateTime(2026, 3, 1)
                });

                await session.StoreAsync(new Order
                {
                    Customer = "Bob",
                    OrderDate = new DateTime(2026, 3, 5)
                });

                await session.StoreAsync(new Order
                {
                    Customer = "Charlie",
                    OrderDate = new DateTime(2026, 3, 10)
                });

                await session.StoreAsync(new Order
                {
                    Customer = "David",
                    OrderDate = new DateTime(2026, 3, 15)
                });

                await session.SaveChangesAsync();
            }
        }

        private class Order
        {
            public string Id { get; set; }
            public string Customer { get; set; }
            public DateTime OrderDate { get; set; }
        }

        private class OutputSchema
        {
            public static OutputSchema Instance = new()
            {
                Answer = "Those are the laptops I found fitting your criteria: Products/5, Products/4, Products/1",
                OrdersIds = new List<string> { "Products/5", "Products/4", "Products/1" }
            };

            public string Answer { get; set; }
            public List<string> OrdersIds { get; set; }

            public override string ToString()
            {
                var ids = OrdersIds != null ? string.Join(", ", OrdersIds) : "null";
                return $"Answer: {Answer} | ProductIds: [{ids}]";
            }
        }
    }
}
