using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_27000 : RavenTestBase
    {
        public RavenDB_27000(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Test(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = CreateShoppingAssistant(config.ConnectionStringName);

            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/1", new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("what are my recent orders?");

            await chat.RunAsync<OutputSchema>(CancellationToken.None);

            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<dynamic>("chats/1");
                var count = doc["SubConversationIds"] is JArray subConversationIds ? subConversationIds.Count : 0;
                Assert.Equal(0, count);
            }
        }

        private class OutputSchema
        {
            public static OutputSchema Instance = new OutputSchema()
            {
                Answer = "the answer to the user question",
                Relevant = true,
                RelevantOrdersId = ["what are the relevant orders?"]
            };

            public string Answer { get; set; }
            public bool Relevant { get; set; }
            public List<string> RelevantOrdersId { get; set; }
        }

        private static AiAgentConfiguration CreateShoppingAssistant(string connectionStringName, bool withActions = false)
        {
            var agent = new AiAgentConfiguration("shopping assistant", connectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

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
    }
}
