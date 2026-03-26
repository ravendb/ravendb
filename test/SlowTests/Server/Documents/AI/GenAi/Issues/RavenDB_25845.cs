using System;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.AI;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_25845(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task GenAi_ShouldHandleNullParameters(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore();

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Name = "GenAi-ShoppingCart-Suggestions";
            config.Identifier = "shopping-cart-suggestions";
            config.Collection = "ShoppingCarts";
            config.Prompt = "assist in generating shopping cart suggestions";
            config.JsonSchema = ChatCompletionClient.GetSchemaFromSampleObject(JsonConvert.SerializeObject(new { Suggestions = new[] { "milk", "bread" } }));

            config.GenAiTransformation = new GenAiTransformation
            {
                Script =
                    "ai.genContext({ " +
                    "  customerId: this.CustomerId, " +
                    "  cartItems: this.Items " +
                    "});"
            };

            config.UpdateScript = "this.SuggestedItems = $output.Suggestions;";
            store.Maintenance.Send(new AddGenAiOperation(config));

            var etlDone = Etl.WaitForEtlToComplete(store, (_, statistics) => statistics.LoadSuccesses > 0);

            using (var session = store.OpenSession())
            {
                session.Store(new ShoppingCart
                {
                    // no items in the cart
                    Id = "carts/1-A",
                    CustomerId = "customers/1-A",
                    Items = null
                });
                session.SaveChanges();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromSeconds(60)), await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));
        }

        private class ShoppingCart
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string[] Items { get; set; }
        }
    }
}
