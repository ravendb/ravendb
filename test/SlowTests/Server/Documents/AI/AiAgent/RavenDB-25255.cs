using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents.AI;
using SlowTests.Server.Documents.AI.GenAi;
using Tests.Infrastructure;
using Xunit;
using static SlowTests.Server.Documents.AI.AiAgent.AiAgentClientApiBasics;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_25255 : RavenTestBase
    {
        public RavenDB_25255(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanUseQueryToolWithSecuredServer(Options options, GenAiConfiguration config)
        {
            var dbName = GetDatabaseName();
            var certificates = Certificates.SetupServerAuthentication();
            var adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var clientCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin
            });

            using var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName
            });

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            agent.Parameters.Add(new AiAgentParameter("company"));
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

            var identifier = (await store.AI.CreateAgentAsync(agent, AnswerSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(identifier, "chats/",
                new AiConversationCreationOptions()
                    .AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("what goes well with my cheese?");
            var r = await chat.RunAsync<AnswerSchema>();
            Assert.Equal(AiConversationResult.Done, r.Status);

            Assert.NotNull(r.Answer);
            Assert.NotNull(chat.Id);

            chat.SetUserPrompt("what goes well with my cheese?");
            r = await chat.RunAsync<AnswerSchema>();
            Assert.Equal(AiConversationResult.Done, r.Status);

            Assert.NotNull(r.Answer);

            chat.SetUserPrompt("what cheese goes well with italian food?");
            r = await chat.RunAsync<AnswerSchema>();
            Assert.Equal(AiConversationResult.Done, r.Status);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanUseQueryToolsInGenAiTaskSecured(Options options, GenAiConfiguration config)
        {
            var dbName = GetDatabaseName();
            var certificates = Certificates.SetupServerAuthentication();
            var cert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var store = GetDocumentStore(new Options
            {
                AdminCertificate = cert,
                ClientCertificate = cert,
                ModifyDatabaseName = s => dbName
            });

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

            await Etl.AssertEtlDoneAsync(etlDone, TimeSpan.FromSeconds(60), store.Database, config);

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
