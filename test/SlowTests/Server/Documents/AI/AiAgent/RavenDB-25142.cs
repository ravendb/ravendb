using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_25142 : RavenTestBase
    {
        public RavenDB_25142(ITestOutputHelper output) : base(output)
        {
        }

        private class AgentResponse
        {
            public string Request { get; set; } = "";
            public string Response { get; set; } = "";
            public string CustomerId { get; set; } = "";
            public string[] RelatedProducts { get; set; } = [""];
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Can_recreate_negative_total_tokens_with_truncation(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentConfig = new AiAgentConfiguration("agent-007", config.ConnectionStringName, "you are agent used to help with finding orders")
            {
                SampleObject = JsonConvert.SerializeObject(new AgentResponse())
            };
            await store.AI.CreateAgentAsync(agentConfig);

            var conversation = store.AI.Conversation("agent-007", "chats/", new AiConversationCreationOptions());

            conversation.AddUserPrompt(["Hi", "Hello", "HEHE"]);

            await conversation.RunAsync<object>();


            conversation.AddUserPrompt(["What would you recommend with cheese?", "What are some great cheese and beer pairings to try, like how a sharp cheddar goes well with an IPA or a creamy brie enhances a stout?"]);

            await conversation.RunAsync<object>();

            conversation.AddUserPrompt(["What would you recommend with meat?", "What are some great chips and fish pairings to try, like how a sharp cheddar goes well with an IPA or a creamy brie enhances a stout?"]);
            conversation.AddUserPrompt(["Answer: HI"]);
            await conversation.RunAsync<object>();
            
            using (var session = store.OpenAsyncSession())
            {
                var convDoc = await session.LoadAsync<BlittableJsonReaderObject>(conversation.Id);

                convDoc.TryGet("Messages", out BlittableJsonReaderArray messages);
                Assert.NotNull(messages);

                var lastMessage = messages.Last() as BlittableJsonReaderObject;
                Assert.NotNull(lastMessage);

                lastMessage.TryGet("usage", out BlittableJsonReaderObject usageJson);
                Assert.NotNull(usageJson);

                usageJson.TryGet("TotalTokens", out long totalTokens);
                Assert.True(totalTokens > 0,
                    $"BUG REPRODUCED: Expected TotalTokens to be positive, and it was {totalTokens}.");
                usageJson.TryGet("PromptTokens", out long promptTokens);
                Assert.True(promptTokens > 0,
                    $"BUG REPRODUCED: Expected PromptTokens to be positive, and it was {promptTokens}.");
            }
        }
    }
}

