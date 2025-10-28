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
using Xunit.Abstractions;

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
        public async Task SendToModelVariants(Options options, GenAiConfiguration configuration, bool? exposed, bool expectEcho, bool expectRaw)
        {
            using var store = GetDocumentStore(options);
            await PutConn(store, configuration);

            var agent = CreateShoppingAssistant(configuration.ConnectionStringName, exposed);
            var agentId = (await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/",
                new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("hello");
            var result = await chat.RunAsync<AiAgentBasics.OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, result.Status);

            await AssertConversationAsync(store, chat.Id,
                expectEcho: expectEcho,
                expectRawValueLeak: expectRaw,
                expectParamStored: true);
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

            Assert.True(conv.TryGet(nameof(ConversationDocument.Parameters), out BlittableJsonReaderObject p));
            Assert.True(p.TryGet("company", out string company));
            Assert.True(p.TryGet("userId", out string userId));
            Assert.Equal("companies/90-A", company);
            Assert.Equal("users/1-A", userId);

            Assert.True(conv.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray msgs));
            var content = string.Join("\n", msgs.Items.Select(i =>
            {
                var o = (BlittableJsonReaderObject)i;
                return o.TryGet("content", out string c) ? c : null;
            }).Where(x => x != null));

            Assert.Contains("AI Agent Parameters:", content);
            Assert.Contains("company = companies/90-A", content);
            Assert.DoesNotContain("userId =", content);
            Assert.DoesNotContain("users/1-A", content);
        }

        private static async Task PutConn(IDocumentStore store, GenAiConfiguration cfg) =>
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(cfg.Connection));

        private static async Task AssertConversationAsync(IDocumentStore store, string id,
            bool expectEcho, bool expectRawValueLeak, bool expectParamStored)
        {
            using var s = store.OpenAsyncSession();
            var conv = await s.LoadAsync<BlittableJsonReaderObject>(id);
            Assert.NotNull(conv);

            Assert.True(conv.TryGet(nameof(ConversationDocument.Parameters), out BlittableJsonReaderObject convParams));
            var hasParam = convParams.TryGet("company", out string companyValue);
            Assert.Equal(expectParamStored, hasParam);
            if (expectParamStored)
                Assert.Equal("companies/90-A", companyValue);

            Assert.True(conv.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray msgs));
            var content = string.Join("\n", msgs.Items.Select(i =>
            {
                var o = (BlittableJsonReaderObject)i;
                return o.TryGet("content", out string c) ? c : null;
            }).Where(x => x != null));

            var echoed = content.Contains("AI Agent Parameters:");
            var leaked = content.Contains("companies/90-A");

            Assert.Equal(expectEcho, echoed);
            Assert.Equal(expectRawValueLeak, leaked);
        }

        private static AiAgentConfiguration CreateShoppingAssistant(string csName, bool? exposed)
        {
            var agent = new AiAgentConfiguration("shopping assistant", csName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            if (exposed.HasValue == false)
                agent.Parameters.Add(new AiAgentParameter("company", "Tenant company id"));
            else
                agent.Parameters.Add(new AiAgentParameter("company", "Tenant company id", exposed.Value));


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
