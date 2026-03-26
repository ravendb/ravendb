using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.AI;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_24972(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanUseQueryToolsInGenAiTask(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore();

            await new ProductsByCategory().ExecuteAsync(store);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Name = "GenAi-With-QueryTool";
            config.Identifier = "products-category-counter";
            config.Collection = "Products";
            config.Prompt = "Figure out if the provided products' category is popular or not. " +
                            "'Popular' is if there are at least 3 Product records of this Category in the dataset. " +
                            "If less than 3, then it isn't considered a popular category." +
                            "Use provided tools.";

            var sampleObject = JsonConvert.SerializeObject(new { IsPopular = true });
            config.JsonSchema = ChatCompletionClient.GetSchemaFromSampleObject(sampleObject);

            config.GenAiTransformation = new GenAiTransformation
            {
                Script = "ai.genContext({ Category: this.Category });"
            };

            config.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "products-by-category",
                    Description = "Return count of products filtered by category",
                    Query = "from index 'ProductsByCategory' where Category = $category select Count",
                    ParametersSampleObject = "{\"category\":\"Vegetables\"}",
                }
            ];

            config.UpdateScript = "this.Popular = $output.IsPopular;";

            store.Maintenance.Send(new AddGenAiOperation(config));

            var etlDone = Etl.WaitForEtlToComplete(store);

            using (var session = store.OpenSession())
            {
                // fruit 
                session.Store(new Product
                {
                    Name = "apple",
                    Category = "fruit"
                });

                // grains
                session.Store(new Product
                {
                    Name = "rice",
                    Category = "grains"
                });
                session.Store(new Product
                {
                    Name = "rye",
                    Category = "grains"
                });

                // alcohol
                session.Store(new Product
                {
                    Name = "beer",
                    Category = "alcohol"
                });
                session.Store(new Product
                {
                    Name = "wine",
                    Category = "alcohol"
                });
                session.Store(new Product
                {
                    Name = "gin",
                    Category = "alcohol"
                });
                session.Store(new Product
                {
                    Name = "vodka",
                    Category = "alcohol"
                });


                session.Advanced.WaitForIndexesAfterSaveChanges();
                session.SaveChanges();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

            using (var session = store.OpenSession())
            {
                var allProducts = session.Query<Product>().ToList();
                Assert.Equal(7, allProducts.Count);

                foreach (var p in allProducts)
                {
                    // only "alcohol" category is popular (4 products)
                    Assert.Equal(p.Category == "alcohol", p.Popular);
                }
            }
        }

        private class Product
        {
            public string Name { get; set; }
            public string Category { get; set; }
            public bool Popular { get; set; }
        }

        private class ProductsByCategory : AbstractIndexCreationTask<Product, ProductsByCategory.Result>
        {
            public class Result
            {
                public string Category { get; set; }
                public int Count { get; set; }
            }

            public ProductsByCategory()
            {
                Map = products => from p in products
                    select new { p.Category, Count = 1 };

                Reduce = results => from r in results
                    group r by r.Category
                    into g
                    select new
                    {
                        Category = g.Key,
                        Count = g.Sum(x => x.Count)
                    };
            }
        }
    }
}
