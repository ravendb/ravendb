using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_24635 : RavenTestBase
    {
        public RavenDB_24635(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ToolCallsWithNoEmptyContentShouldntFail(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("my assistant", config.ConnectionStringName,
                "You are a tool-calling assistant." +
                "When you perform a tool call, always include a short explanation to the user in the `content` field about what is about to happen, and then in the `tool_calls` field the parameters for the call.");

            agent.Parameters.Add(new AiAgentParameter("company"));
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

            var identifier = (await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(identifier, "chats/",
                new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("run tool 'RecentOrder'");
            await chat.RunAsync<AiAgentBasics.OutputSchema>(CancellationToken.None);
            chat.SetUserPrompt("run tool 'RecentOrder'");
            await chat.RunAsync<AiAgentBasics.OutputSchema>(CancellationToken.None); 
        }
    }
}
