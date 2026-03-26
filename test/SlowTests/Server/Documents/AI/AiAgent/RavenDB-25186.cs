using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_25186 : RavenTestBase
    {
        public RavenDB_25186(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true, true])]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [false, false, false])]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [null, true, true])]
        public async Task SendToModelVariants(Options options, GenAiConfiguration configuration, bool? sendToModel, bool expectEcho, bool expectRaw)
        {
            using var store = GetDocumentStore(options);
            await PutConn(store, configuration);

            var agent = CreateShoppingAssistant(configuration.ConnectionStringName, sendToModel);
            var agentId = (await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/",
                new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("hello");
            var result = await chat.RunAsync<AiAgentBasics.OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, result.Status);

            await AssertConversationAsync(store, chat.Id,
                shouldProjectParams: expectEcho,
                shouldProjectCompanyParam: expectRaw);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task MixedParametersOnlyExposedParametersAreInTheContent(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            await PutConn(store, configuration);

            var agent = CreateShoppingAssistant(configuration.ConnectionStringName, true);
            agent.Parameters.Add(new AiAgentParameter("userId", "Authenticated user identifier", sendToModel: false));
            var agentId = (await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/",
                new AiConversationCreationOptions()
                    .AddParameter("company", "companies/90-A")
                    .AddParameter("userId", "users/1-A"));

            chat.SetUserPrompt("hello");
            var result = await chat.RunAsync<AiAgentBasics.OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, result.Status);

            using var s = store.OpenAsyncSession();
            var conv = await s.LoadAsync<BlittableJsonReaderObject>(chat.Id);

            Assert.True(conv.TryGet(nameof(ConversationDocument.Parameters), out BlittableJsonReaderObject parameters));
            Assert.True(parameters.TryGet("company", out BlittableJsonReaderObject companyAiConversationValue));
            Assert.True(companyAiConversationValue.TryGet("Value", out string companyValue));
            Assert.Equal("companies/90-A", companyValue);


            Assert.True(parameters.TryGet("userId", out BlittableJsonReaderObject userIdAiConversationValue));
            Assert.True(userIdAiConversationValue.TryGet("Value", out string userIdValue));
            Assert.Equal("users/1-A", userIdValue);

            Assert.True(conv.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray messages));
            var messagesContents = messages.Items.Select(i => {
                var o = (BlittableJsonReaderObject)i;
                return o.TryGet("content", out string c) ? c : null;
            }).Where(x => x != null).ToList();

            Assert.True(messagesContents.Any(msg => msg.Contains("AI Agent Parameters:")));
            Assert.True(messagesContents.Any(msg => msg.Contains("company = companies/90-A")));
            Assert.False(messagesContents.Any(msg => msg.Contains("userId =")));
            Assert.False(messagesContents.Any(msg => msg.Contains("users/1-A")));
        }

        private static async Task PutConn(IDocumentStore store, GenAiConfiguration cfg) =>
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(cfg.Connection));

        private static async Task AssertConversationAsync(IDocumentStore store, string id,
            bool shouldProjectParams, bool shouldProjectCompanyParam)
        {
            using (var session = store.OpenAsyncSession())
            {
                var chat = await session.LoadAsync<BlittableJsonReaderObject>(id);
                Assert.True(chat.TryGet(nameof(ConversationDocument.Parameters), out BlittableJsonReaderObject parameters));
                Assert.True(parameters.TryGet("company", out BlittableJsonReaderObject companyAiConversationValue));
                Assert.True(companyAiConversationValue.TryGet("Value", out string companyValue));
                Assert.Equal("companies/90-A", companyValue);

                Assert.True(chat.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray messages));
                var messagesContents = messages.Items.Select(i =>
                {
                    var o = (BlittableJsonReaderObject)i;
                    return o.TryGet("content", out string c) ? c : null;
                }).Where(x => x != null).ToList();

                var parametersProjectedInMessages = messagesContents.Any(msg => msg.Contains("AI Agent Parameters:"));
                var companyParameterProjectedInMessages = messagesContents.Any(msg => msg.Contains("companies/90-A"));

                Assert.Equal(shouldProjectParams, parametersProjectedInMessages);
                Assert.Equal(shouldProjectCompanyParam, companyParameterProjectedInMessages);
            }
        }

        private static AiAgentConfiguration CreateShoppingAssistant(string csName, bool? sendToModel)
        {
            var agent = new AiAgentConfiguration("shopping assistant", csName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            if (sendToModel.HasValue == false)
                agent.Parameters.Add(new AiAgentParameter("company", "Tenant company id"));
            else
                agent.Parameters.Add(new AiAgentParameter("company", "Tenant company id", sendToModel.Value));

            agent.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSampleObject = "{}"
                }
            ];
            return agent;
        }
    }
}
