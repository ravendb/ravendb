using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_25993(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task AgentParameterTypeTest(Options options, GenAiConfiguration config)
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
            productsSearchAgent.Parameters.Add(new AiAgentParameter("maxBudgetNis", "Max budget (NIS)") { Type = AiAgentParameterValueType.Number });
            productsSearchAgent.Parameters.Add(new AiAgentParameter("minRamGb", "Min RAM (GB)") { Type = AiAgentParameterValueType.Number });
            var productsSearchId = (await store.AI.CreateAgentAsync(productsSearchAgent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(
                productsSearchId,
                "Chats/1",
                new AiConversationCreationOptions().AddParameter("maxBudgetNis", "3500").AddParameter("minRamGb", 16)
            );

            chat.SetUserPrompt(
                "Find laptops that'll fit to me needs."
            );
            var e = await Assert.ThrowsAsync<AiException>(() => chat.RunAsync<OutputSchema>());
            Assert.Contains("Parameter 'maxBudgetNis' has invalid type. Expected: Number, Actual: String, Value: 3500", e.Message);

            chat = store.AI.Conversation(
                productsSearchId,
                "Chats/2",
                new AiConversationCreationOptions().AddParameter("maxBudgetNis", 3500).AddParameter("minRamGb", 16)
            );
            
            chat.SetUserPrompt("Find laptops that'll fit to me needs.");
            var r = await chat.RunAsync<OutputSchema>();
        }


        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task AgentParameterTypeTest_Primitives(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // products-search-agent
            var productsSearchAgent = new AiAgentConfiguration(
                "products-search-agent",
                config.ConnectionStringName,
                "You are a product search agent!"
            );
            productsSearchAgent.Parameters.Add(new AiAgentParameter("stringParam", "some param") { Type = AiAgentParameterValueType.String });
            productsSearchAgent.Parameters.Add(new AiAgentParameter("numberParam", "some param") { Type = AiAgentParameterValueType.Number });
            productsSearchAgent.Parameters.Add(new AiAgentParameter("booleanParam", "some param") { Type = AiAgentParameterValueType.Boolean });
            var productsSearchId = (await store.AI.CreateAgentAsync(productsSearchAgent, OutputSchema.Instance)).Identifier;

            var creationOptions = new AiConversationCreationOptions().AddParameter("stringParam", "abc")
                                                                        .AddParameter("numberParam", 16)
                                                                        .AddParameter("booleanParam", true);

            await Talk("Chats/1"); // should pass

            creationOptions = new AiConversationCreationOptions().AddParameter("stringParam", 1)
                .AddParameter("numberParam", 16)
                .AddParameter("booleanParam", true);
            var e = await Assert.ThrowsAsync<AiException>(() => Talk("Chats/2"));
            Assert.Contains("Parameter 'stringParam' has invalid type. Expected: String, Actual: Number, Value: 1", e.Message);
            
            creationOptions = new AiConversationCreationOptions().AddParameter("stringParam", "abc")
                .AddParameter("numberParam", "16") // should throw
                .AddParameter("booleanParam", true);
            e = await Assert.ThrowsAsync<AiException>(() => Talk("Chats/3"));
            Assert.Contains("Parameter 'numberParam' has invalid type. Expected: Number, Actual: String, Value: 16", e.Message);
            
            creationOptions = new AiConversationCreationOptions().AddParameter("stringParam", "abc")
                .AddParameter("numberParam", 16)
                .AddParameter("booleanParam", 5);  // should throw
            e = await Assert.ThrowsAsync<AiException>(() => Talk("Chats/4"));
            Assert.Contains("Parameter 'booleanParam' has invalid type. Expected: Boolean, Actual: Number, Value: 5", e.Message);
            
            async Task Talk(string chatId)
            {
                var chat = store.AI.Conversation(
                    productsSearchId,
                    chatId,
                    creationOptions
                );
                chat.SetUserPrompt("Who are you.");
                var r = await chat.RunAsync<OutputSchema>();
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task AgentParameterTypeTest_Array(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // products-search-agent
            var productsSearchAgent = new AiAgentConfiguration(
                "products-search-agent",
                config.ConnectionStringName,
                "You are a product search agent!"
            );
            productsSearchAgent.Parameters.Add(new AiAgentParameter("arrParam", "some param") { Type = AiAgentParameterValueType.ArrayOfNumber });
            var productsSearchId = (await store.AI.CreateAgentAsync(productsSearchAgent, OutputSchema.Instance)).Identifier;

            var creationOptions = new AiConversationCreationOptions().AddParameter("arrParam", new int[] { 1, 2, 3 }); // shouldn't throw
            await Talk("Chats/1");

            creationOptions = new AiConversationCreationOptions().AddParameter("arrParam", new OutputSchema[] { new OutputSchema() { Answer = "ans" } }); // should throw
            var e = await Assert.ThrowsAsync<InvalidOperationException>(() => Talk("Chats/2")); // from client side, because array is not supported at all
            Assert.Contains("Got unknown type", e.Message);

            creationOptions = new AiConversationCreationOptions().AddParameter("arrParam", new object[] { 1, 2, 3 }); // shouldn't throw
            await Talk("Chats/3");

            creationOptions = new AiConversationCreationOptions().AddParameter("arrParam", new object[] { 1, 2, new OutputSchema() }); // should throw
            var e2 = await Assert.ThrowsAsync<InvalidOperationException>(() => Talk("Chats/4")); // from client side, because array is not supported at all
            Assert.Contains("Got unknown type", e2.Message);

            creationOptions = new AiConversationCreationOptions().AddParameter("arrParam", new object[] { 1, 2, "abc" }); // should throw
            var e3 = await Assert.ThrowsAsync<AiException>(() => Talk("Chats/5"));
            Assert.Contains("Parameter 'arrParam' has unsupported type. Actual: Array of mixed element types: 'Number' and 'String'.", e3.Message);

            creationOptions = new AiConversationCreationOptions().AddParameter("arrParam", new object[] { null }); // should throw
            var e4 = await Assert.ThrowsAsync<AiException>(() => Talk("Chats/6"));
            Assert.Contains("Parameter 'arrParam' has unsupported type. Actual: Array of unsupported element type 'Null'.", e4.Message);

            async Task Talk(string chatId)
            {
                var chat = store.AI.Conversation(
                    productsSearchId,
                    chatId,
                    creationOptions
                );
                chat.SetUserPrompt("Who are you.");
                var r = await chat.RunAsync<OutputSchema>();
            }
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

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task MultiAgentStringParametersInsteadOfNumberBug(Options options, GenAiConfiguration config)
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
            var productsSearchId = (await store.AI.CreateAgentAsync(productsSearchAgent, OutputSchema.Instance)).Identifier;

            // root agent
            var root = new AiAgentConfiguration(
                "store-clerk-agent",
                config.ConnectionStringName,
                "You are the store clerk (root). " +
                "Use products-search-agent to get product ids. "
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent { Identifier = productsSearchId, Description = "Find products" },
                ]
            };

            var rootId = (await store.AI.CreateAgentAsync(root, OutputSchema.Instance)).Identifier;
            var chat = store.AI.Conversation(
                rootId,
                "Chats/1",
                new AiConversationCreationOptions().AddParameter("customerId", "Customers/1").AddParameter("creditCardNumber", "1234-1234-1234-1234")
            );
            chat.SetUserPrompt(
                "My criteria: Budget 3500 NIS, min 16GB RAM. " +
                "Find laptops for me."
            );
            var r1 = await chat.RunAsync<OutputSchema>();
            Assert.Equal(3, r1.Answer.ProductIds.Count);
        }


        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task MultiAgentTypeConflict(Options options, GenAiConfiguration config)
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
                Type = AiAgentParameterValueType.Number
            });
            var productsSearchId = (await store.AI.CreateAgentAsync(productsSearchAgent, OutputSchema.Instance)).Identifier;

            // root agent
            var root = new AiAgentConfiguration(
                "store-clerk-agent",
                config.ConnectionStringName,
                "You are the store clerk (root). " +
                "Use products-search-agent to get product ids. "
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
                new AiConversationCreationOptions().AddParameter("customerId", "Customers/1").AddParameter("creditCardNumber", "1234-1234-1234-1234")
            );
            chat.SetUserPrompt(
                "My criteria: Budget 3500 NIS, min 16GB RAM. " +
                "Find laptops for me."
            );
            var e = await Assert.ThrowsAsync<MissingAiAgentParameterException>(() => chat.RunAsync<OutputSchema>());
            Assert.Contains("Parameter 'customerId' has mismatched types between parent and sub-agent. Parent type: 'String', Sub-agent type: 'Number'. Both must declare the same ValueType.", e.Message);
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
    }
}
