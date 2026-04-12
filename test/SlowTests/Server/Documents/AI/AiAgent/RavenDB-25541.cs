using System.Linq;
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
    public class RavenDB_25541 : RavenTestBase
    {
        public RavenDB_25541(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ReceiveAndHandleCalledOnce(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("my assistant", config.ConnectionStringName,
                "Always call both tools- tool 'whoami' and the tool 'randomDocument' tools to gather context before answering the user.")
            {
                Actions =
                [
                    new AiAgentToolAction
                    {
                        Name = "whoami",
                        ParametersSampleObject = "{}",
                        Description = "Describe who this user is, preferences, etc"
                    },
                    new AiAgentToolAction
                    {
                        Name = "randomDocument",
                        ParametersSampleObject = "{}",
                        Description = "Get a random document from the database"
                    }
                ]
            };

            var identifier = (await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance)).Identifier;
            var chat = store.AI.Conversation(identifier, "chats/",
                new AiConversationCreationOptions());

            int receiveCalled = 0;
            int handleCalled = 0;
            chat.Receive<object>("whoami", (a, args) =>
            {
                receiveCalled++;
            });

            chat.Handle<object>("randomDocument", (a, args) =>
            {
                handleCalled++;
                return new object();
            });

            chat.SetUserPrompt("Give me 15 real cities names, one per line");
            var r = await chat.RunAsync<AiAgentBasics.OutputSchema>();
            Assert.Equal(1, handleCalled);
            Assert.Equal(1, receiveCalled);
            Assert.Equal(AiConversationResult.ActionRequired, r.Status);

            await chat.RunAsync<AiAgentBasics.OutputSchema>();
            Assert.Equal(2, receiveCalled);
        }
    }
}
