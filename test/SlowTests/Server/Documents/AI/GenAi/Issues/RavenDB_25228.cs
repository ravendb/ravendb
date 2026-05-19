using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL;
using Raven.Server.NotificationCenter.Notifications.Details;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_25228(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenRetryTheory(RavenTestCategory.Ai, maxRetries: 3, delayBetweenRetriesMs:10_000)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanPassAgentParametersIntoGenAiTools(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore();

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // --- GenAI task config ---
            config.Name = "GenAi-ShoppingCart-Suggestions";
            config.Identifier = "shopping-cart-suggestions";
            config.Collection = "ShoppingCarts";

            config.Prompt =
                "execute tool 'recent-orders-for-customer' ONCE to fetch the list of items from the customer's recent orders.  +\n  " +
                "FROM that list, suggest the rest of the items (the ones that are on the tool list but not on the customer shopping cart) \" +\n               " +
                "Make sure that the items you suggest ARE NOT already in 'cartItems'. ";

            config.JsonSchema = ChatCompletionClient.GetSchemaFromSampleObject(JsonConvert.SerializeObject(new { Suggestions = new[] { "meat" } }));

            config.GenAiTransformation = new GenAiTransformation
            {
                Script =
                    "ai.genContext({ " +
                    "  customerId: this.CustomerId, " +
                    "  cartItems: this.Items " +
                    "});"
            };

            config.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "recent-orders-for-customer",
                    Description = "Return item names from recent orders",
                    Query =
                        "from Orders " +
                        "where CustomerId = $customerId " +
                        "select Items",
                    ParametersSampleObject = "{}",
                }
            ];

            // Update the ShoppingCart with the model's Suggestions
            config.UpdateScript = "this.SuggestedItems = $output.Suggestions;";

            store.Maintenance.Send(new AddGenAiOperation(config));

            var etlDone = Etl.WaitForEtlToComplete(store);

            // --- Seed data ---
            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    Id = "orders/1-A",
                    CustomerId = "customers/1-A",
                    Items = ["milk", "bread"]
                });
                session.Store(new Order
                {
                    Id = "orders/2-A",
                    CustomerId = "customers/1-A",
                    Items = ["eggs"]
                });
                session.Store(new Order
                {
                    Id = "orders/3-A",
                    CustomerId = "customers/1-A",
                    Items = ["wine"]
                });

                session.Store(new Order
                {
                    Id = "orders/4-A",
                    CustomerId = "customers/2-A",
                    Items = ["apples"]
                });

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Store(new ShoppingCart
                {
                    Id = "carts/1-A",
                    CustomerId = "customers/1-A",
                    Items = ["bread", "apples"]
                });

                session.Store(new ShoppingCart
                {
                    Id = "carts/2-A",
                    CustomerId = "customers/2-A",
                    Items = []
                });

                session.SaveChanges();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

            using (var session = store.OpenSession())
            {
                var cart1 = session.Load<ShoppingCart>("carts/1-A");
                var cart2 = session.Load<ShoppingCart>("carts/2-A");

                Assert.NotNull(cart1);
                Assert.NotNull(cart2);

                // For customers/1-A:
                // recent items: milk, bread, eggs, wine
                // cart already has bread, apples → suggestions should be [milk, eggs, wine]
                Assert.NotNull(cart1.SuggestedItems);
                Assert.Equal(3, cart1.SuggestedItems.Length);
                Assert.DoesNotContain("bread", cart1.SuggestedItems);
                Assert.Contains("milk", cart1.SuggestedItems);
                Assert.Contains("eggs", cart1.SuggestedItems);
                Assert.Contains("wine", cart1.SuggestedItems);

                // For customers/2-A:
                // recent items: apples; empty cart → suggestions should be [apples]
                Assert.NotNull(cart2.SuggestedItems);
                Assert.Single(cart2.SuggestedItems);
                Assert.Equal("apples", cart2.SuggestedItems[0]);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldThrowWhenParametersInQueryToolAreNotPassedInContext(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore();

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Name = "GenAi-ShoppingCart-Suggestions";
            config.Identifier = "shopping-cart-suggestions";
            config.Collection = "ShoppingCarts";

            config.Prompt =
                "Use the tool 'recent-orders-for-customer' to fetch items from the customer's recent orders. " +
                "From that list, suggest items to add to the shopping cart. " +
                "Make sure that the items you suggest are NOT already in 'cartItems'. ";

            config.JsonSchema = ChatCompletionClient.GetSchemaFromSampleObject(JsonConvert.SerializeObject(new { Suggestions = new[] { "milk", "bread" } }));

            // customerId is not part of the context object that we send to the model 
            config.GenAiTransformation = new GenAiTransformation
            {
                Script =
                    "ai.genContext({ " +
                    "  cartItems: this.Items " +
                    "});"
            };

            config.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "recent-orders-for-customer",
                    Description = "Return item names from recent orders",
                    Query =
                        "from Orders " +
                        "where CustomerId = $customerId " +
                        "select Items",
                    ParametersSampleObject = "{}",
                }
            ];

            // Update the ShoppingCart with the model's deterministic Suggestions
            config.UpdateScript = "this.SuggestedItems = $output.Suggestions;";

            store.Maintenance.Send(new AddGenAiOperation(config));

            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    Id = "orders/1-A",
                    CustomerId = "customers/1-A",
                    Items = new[] { "milk", "bread" }
                });
                session.Store(new ShoppingCart
                {
                    Id = "carts/1-A",
                    CustomerId = "customers/1-A",
                    Items = new[] { "bread", "apples" }
                });
                session.SaveChanges();
            }

            IEnumerable<TaskProcessErrorTableValue> errors = null;
            var value = await WaitForValueAsync(async () =>
            {
                errors = await Etl.GetProcessLoadErrorsAsync(store.Database, config);
                return errors.Any();
            }, true, timeout: 60_000);

            Assert.True(value, await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));
            Assert.NotEmpty(errors);
            Assert.True(errors.First().Error.Contains("Tool query 'recent-orders-for-customer' contains parameters that are not defined in the agent configuration: 'customerId'"));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldThrowWhenParameterIsDefinedInQueryToolSchemaAndPassedInContext(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore();

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Name = "GenAi-ShoppingCart-Suggestions";
            config.Identifier = "shopping-cart-suggestions";
            config.Collection = "ShoppingCarts";

            config.Prompt =
                "Use the tool 'recent-orders-for-customer' to fetch items from the customer's recent orders. " +
                "From that list, suggest items to add to the shopping cart. " +
                "Make sure that the items you suggest are NOT already in 'cartItems'. ";

            config.JsonSchema = ChatCompletionClient.GetSchemaFromSampleObject(JsonConvert.SerializeObject(new { Suggestions = new[] { "milk", "bread" } }));

            // customerId is part of the context object that we send to the model 
            config.GenAiTransformation = new GenAiTransformation
            {
                Script =
                    "ai.genContext({ " +
                    "  customerId: this.CustomerId, " +
                    "  cartItems: this.Items " +
                    "});"
            };

            // but customerId also appears as a parameter in ParametersSampleObject
            config.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "recent-orders-for-customer",
                    Description = "Return item names from recent orders",
                    Query =
                        "from Orders " +
                        "where CustomerId = $customerId " +
                        "select Items",
                    ParametersSampleObject = "{\"customerId\":\"customers/1-A\"}",
                }
            ];

            config.UpdateScript = "this.SuggestedItems = $output.Suggestions;";
            store.Maintenance.Send(new AddGenAiOperation(config));

            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    Id = "orders/1-A",
                    CustomerId = "customers/1-A",
                    Items = new[] { "milk", "bread" }
                });
                session.Store(new ShoppingCart
                {
                    Id = "carts/1-A",
                    CustomerId = "customers/1-A",
                    Items = new[] { "bread", "apples" }
                });
                session.SaveChanges();
            }

            IEnumerable<TaskProcessErrorTableValue> errors = null;
            var value = await WaitForValueAsync(async () =>
            {
                errors = await Etl.GetProcessLoadErrorsAsync(store.Database, config);
                return errors.Any();
            }, true, timeout: 60_000);

            Assert.True(value, await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));
            Assert.NotEmpty(errors);
            Assert.True(errors.First().Error.Contains("Parameter customerId is defined on both the agent level and the query level for recent-orders-for-customer"));
        }

        private class Order
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string[] Items { get; set; }
        }

        private class ShoppingCart
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string[] Items { get; set; }
            public string[] SuggestedItems { get; set; }
        }
    }
}
