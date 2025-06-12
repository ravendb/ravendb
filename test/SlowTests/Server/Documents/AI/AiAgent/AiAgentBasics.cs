using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.AiAgent;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentBasics : RavenTestBase
    {

        public class OutputSchema
        {
            public string Answer = "Answer to the user question";

            public bool Relevant = true;

            public List<string> RelevantOrdersId = ["The order ids relevant to the query or response"];

            public List<string> MatchingProductsId = ["All the product ids referenced either by the user or the system"];
        }

        public AiAgentBasics(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task CanCreateAiAgent(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration(config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            agent.Persistence = new AiAgentConfiguration.PersistenceConfiguration
            {
                Collection = "Chats",
                Expires = TimeSpan.FromDays(30)
            };

            agent.Queries =
            [
                new AiAgentConfiguration.ToolQuery
                {
                    Name = "ProductSearch",
                    Description = "semantic search the store product catalog",
                    Query = session.Query<Product>().VectorSearch(v=>v.WithText(p=>p.Name), v=>v.ByText("$query")).ToString(),
                    // Query = "from Products where vector.search(embedding.text(Name), $query)",
                    ParametersSchema = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                },

                new AiAgentConfiguration.ToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    // Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    Query = session.Query<Query.Order>().Where(o => o.Company == "$company").OrderByDescending(o => o.OrderedAt).Take(10).ToString(),
                    ParametersSchema = "{}"
                }
            ];

            await store.Maintenance.SendAsync(new AddOrModifyAiAgentOperation<OutputSchema>("shopping assistant", agent));
            var r = await store.Maintenance.SendAsync(new StartChatOperation<OutputSchema>("shopping assistant", "what goes well with my cheese?",
                new Dictionary<string, object> { ["company"] = "companies/90-A" }));

            Assert.NotNull(r.Response.Answer);
            Assert.NotNull(r.Usage);
            Assert.NotNull(r.ChatId);

            var chat = await session.LoadAsync<dynamic>(r.ChatId);
            Assert.NotNull(chat);
        }
    }
}
