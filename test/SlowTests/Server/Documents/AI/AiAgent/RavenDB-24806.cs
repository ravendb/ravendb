using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
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
    public class RavenDB_24806 : RavenTestBase
    {
        public RavenDB_24806(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Test(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var trimmingConfig = new AiAgentChatTrimmingConfiguration
            {
                Truncate = new AiAgentTruncateChat()
                {
                    MessagesTokensBeforeTruncate = 500,
                    MessagesTokensAfterTruncate = 300
                }
            };

            var agentConfig = new AiAgentConfiguration("truncation-tester", config.ConnectionStringName, "You are a simple echo bot. Repeat the user's message exactly as it was given to you.")
            {
                ChatTrimming = trimmingConfig,
                SampleObject = JsonConvert.SerializeObject(new { answer = "string" })
            };
            var agent = await store.AI.CreateAgentAsync(agentConfig);

            var conversation = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());

            var longPrompt = "This is a reasonably long user prompt designed to generate a response that consumes a good number of tokens to properly test the truncation feature.";

            for (int i = 0; i < 10; i++)
            {
                conversation.SetUserPrompt($"{longPrompt} - Message number {i + 1}");
                await conversation.RunAsync<object>();
            }

            using (var session = store.OpenAsyncSession())
            {
                var convoDoc = await session.LoadAsync<BlittableJsonReaderObject>(conversation.Id);

                convoDoc.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray messages);
                Assert.NotNull(messages);

                Assert.True(messages.Length < 21, $"Expected fewer than 21 messages after truncation, but found {messages.Length}. This indicates truncation did not occur.");

                convoDoc.TryGet(nameof(ConversationDocument.CurrentUsage), out BlittableJsonReaderObject usageJson);
                Assert.NotNull(usageJson);

                usageJson.TryGet(nameof(AiUsage.TotalTokens), out long finalTokenCount);

                Assert.True(finalTokenCount < 500, $"Final token count ({finalTokenCount}) should be less than the trigger limit (500).");
            }
        }
    }
}
