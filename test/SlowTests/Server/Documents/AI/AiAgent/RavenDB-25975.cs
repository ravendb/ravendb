using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_25975(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task SendToModelOnChat(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            await SeedProducts(store);

            // products-search-agent
            var productsSearchAgent = new AiAgentConfiguration(
                "products-search-agent",
                config.ConnectionStringName,
                "Find top 3 laptops within a budget and minimum RAM. and return all matching results. if you have 3 return 3, not less!"
            )
            {
                Queries = new List<AiAgentToolQuery>
                {
                    new AiAgentToolQuery
                    {
                        Name = "FindTopLaptops",
                        Description = "Return top 3 laptops that match budget and min RAM. Return all of the laptops you found that will fit to the request.",
                        Query =
                            "from Products as p " +
                            "where p.Category = 'Laptop' " +
                            "and p.PriceNis <= $maxBudgetNis " +
                            "and p.RamGb >= $minRamGb " +
                            "order by p.PriceNis asc " +
                            "limit 0, 3",
                        ParametersSampleObject = "{}"
                    }
                }
            };
            productsSearchAgent.Parameters.Add(new AiAgentParameter("maxBudgetNis", "Max budget (NIS)")
            {
                Type = AiAgentParameterValueType.Number
            });
            productsSearchAgent.Parameters.Add(new AiAgentParameter("minRamGb", "Min RAM (GB)")
            {
                Type = AiAgentParameterValueType.Number
            });
            productsSearchAgent.Parameters.Add(new AiAgentParameter("customerId", "Customer Id")
            {
                Type = AiAgentParameterValueType.String
            });
            var productsSearchId = (await store.AI.CreateAgentAsync(productsSearchAgent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(
                productsSearchId,
                "Chats/1",
                new AiConversationCreationOptions().AddParameter("maxBudgetNis", 3500) // send
                    .AddParameter("minRamGb", 16) // send
                    .AddParameter("customerId", "Customers/1", new AiConversationParameterOptions(){ SendToModel = false}) //don't-send
                    // Additional fields for sub-agent
                    .AddParameter("customerName", "Shahar", new AiConversationParameterOptions(){ SendToModel = true}) // send (pass it as is to the sub-agent if exists)
            );
            chat.SetUserPrompt(
                "Find laptops for me."
            );
            var r = await chat.RunAsync<OutputSchema>();

            // customerId shouldn't be sent (sendToModel is false on chat)
            var expectedParamsMsg = "AI Agent Parameters:\nmaxBudgetNis = 3500" + Environment.NewLine +
                                    "minRamGb = 16" + Environment.NewLine +
                                    "customerName = Shahar" + Environment.NewLine;
            var result = await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions
            {
                ConversationId = "Chats/1",
                DetailLevel = AiConversationDetailLevel.Full,
                PageSize = 50
            });
            Assert.Contains(result.Messages, m => m.Content == expectedParamsMsg);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task SendToModelOnChat_MultiAgent(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            await SeedProducts(store);

            // products-search-agent
            var productsSearchAgent = new AiAgentConfiguration(
                "products-search-agent",
                config.ConnectionStringName,
                "Find top 3 laptops. if you have 3 return 3, not less!"
            )
            {
                Queries = new List<AiAgentToolQuery>
                {
                    new AiAgentToolQuery
                    {
                        Name = "FindTopLaptops",
                        Description = "Return top 3 laptops that match customer requirements",
                        Query =
                            "from Products as p " +
                            "where p.Category = 'Laptop' " +
                            "and p.PriceNis <= $maxBudgetNis " +
                            "and p.RamGb >= $minRamGb " +
                            "order by p.PriceNis asc " +
                            "limit 0, 3",
                        ParametersSampleObject = "{}"
                    }
                }
            };
            productsSearchAgent.Parameters.Add(new AiAgentParameter("maxBudgetNis", "Max budget (NIS)")
            {
                Type = AiAgentParameterValueType.Number
            });
            productsSearchAgent.Parameters.Add(new AiAgentParameter("minRamGb", "Min RAM (GB)")
            {
                Type = AiAgentParameterValueType.Number
            });
            var productsSearchId = (await store.AI.CreateAgentAsync(productsSearchAgent, OutputSchema.Instance)).Identifier;

            // root agent
            var root = new AiAgentConfiguration(
                "store-clerk-agent",
                config.ConnectionStringName,
                "You are the store clerk (root). " +
                "Use the `products-search-agent` to retrieve product IDs.\n\nIf the user did not specify any criteria for products or laptops, " +
                "call `products-search-agent` without parameters (or with empty parameters). " +
                "only 'subAgentUserPrompt'  param is mandatory."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent { Identifier = productsSearchId, Description = "Find products" },
                ]
            };
            root.Parameters.Add(new AiAgentParameter("customerId", "Customer Id")
            {
                Type = AiAgentParameterValueType.String
            });

            var rootId = (await store.AI.CreateAgentAsync(root, OutputSchema.Instance)).Identifier;
            var chat = store.AI.Conversation(
                rootId,
                "Chats/1",
                new AiConversationCreationOptions()
                  
                    // for sub-agent only (additional params)
                    .AddParameter("minRamGb", 16, new AiConversationParameterOptions() { SendToModel = false }) // don't-send (only for sub-agent query)
                    .AddParameter("maxBudgetNis", 3500, new AiConversationParameterOptions() { SendToModel = false }) //don't-send to sub-agent (only for sub-agent query)

                    // additional params
                    .AddParameter("customerId", "Customers/1") // additional at the sub-agent - send to root-agent and to the sub-agent
                    .AddParameter("productType", "Laptop", new AiConversationParameterOptions() { SendToModel = true }) // additional param (true) - send to root and sub-agent
                    .AddParameter("someAdditionalParam", "abc", new AiConversationParameterOptions() { SendToModel = false }) // additional param (false) - don't send to root/sub-agent
            );
            chat.SetUserPrompt("Find laptops for me.");
            var r = await chat.RunAsync<OutputSchema>();
            Assert.Equal(AiConversationResult.Done, r.Status);

            var expectedParamsMsg1 = "AI Agent Parameters:\ncustomerId = Customers/1" + Environment.NewLine + "productType = Laptop" + Environment.NewLine;
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<Chat>("Chats/1");
                Assert.Equal(5, doc.Parameters.Count);
                Assert.True(doc.Messages.Any(m => m.Content as string == expectedParamsMsg1));

                var subDoc = (await session.Advanced.LoadStartingWithAsync<Chat>("Chats/1/products-search-agent/")).Single();
                Assert.Equal(5, subDoc.Parameters.Count);
                Assert.True(subDoc.Messages.Any(m => m.Content as string == expectedParamsMsg1));
            }

            // Expected Flow:

            // credit-card true
            // root credit-card false -> false
            // child credit-card true -> true

            // credit-card false
            // root credit-card false -> false
            // child credit-card true -> false

            //Additional Param:
            // credit-card false
            // root          -> false
            // child credit-card true -> false

            // credit-card true
            // root          -> true
            // child credit-card true

            // credit-card true
            // root          -> true
            // child credit-card false -> false

            // credit-card false
            // root          -> false
            // child credit-card false -> false
        }


        private static async Task SeedProducts(IDocumentStore store)
        {
            using var session = store.OpenAsyncSession();

            // Matches (<=3500 & >=16GB): Products/5 (2790), Products/4 (2990), Products/1 (3290)
            await session.StoreAsync(new Product { Id = "Products/1", Category = "Laptop", RamGb = 16, PriceNis = 3290 });
            await session.StoreAsync(new Product { Id = "Products/4", Category = "Laptop", RamGb = 16, PriceNis = 2990 });
            await session.StoreAsync(new Product { Id = "Products/5", Category = "Laptop", RamGb = 16, PriceNis = 2790 });

            // Additional valid (<=3500 & >=16GB) for add flow
            await session.StoreAsync(new Product { Id = "Products/6", Category = "Laptop", RamGb = 24, PriceNis = 3490 });

            // Non-matching examples
            await session.StoreAsync(new Product { Id = "Products/2", Category = "Laptop", RamGb = 32, PriceNis = 4890 }); // above budget
            await session.StoreAsync(new Product { Id = "Products/3", Category = "Laptop", RamGb = 8, PriceNis = 1990 }); // below RAM

            await session.SaveChangesAsync();
        }


        // Entities
        private class Product
        {
            public string Id { get; set; }
            public string Category { get; set; } // "Laptop"
            public int RamGb { get; set; }
            public decimal PriceNis { get; set; }
        }

        // Minimal tool payloads
        private class OutputSchema
        {
            public static OutputSchema Instance = new()
            {
                Answer = "Those are the laptops I found fitting your criteria: Products/5, Products/4, Products/1",
                ProductIds = new List<string> { "Products/5", "Products/4", "Products/1" }
            };

            public string Answer { get; set; }
            public List<string> ProductIds { get; set; }

            public override string ToString()
            {
                var ids = ProductIds != null ? string.Join(", ", ProductIds) : "null";
                return $"Answer: {Answer} | ProductIds: [{ids}]";
            }
        }

        // Chat
        private class Chat
        {
            public Dictionary<string, AiConversationParameter> Parameters { get; set; }
            public List<Message> Messages { get; set; }
        }

        private class Message
        {
            [JsonProperty("role")]
            public string Role { get; set; }

            [JsonProperty("content")]
            public object Content { get; set; }
        }
    }
}
