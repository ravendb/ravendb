using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_24913(ITestOutputHelper output) : RavenTestBase(output)
{
    private record User(string Username, string FirstName, string LastName);

    private record Reply(string Answer);
    
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanProvideInitialContextToQuery(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User("ayende", "Oren", "Eini"));
            await session.SaveChangesAsync();
        }
        
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));


        AiAgentConfiguration CreateAgent(AiAgentToolQueryOptions toolQueryOptions)
        {
            var aiAgentConfiguration = new AiAgentConfiguration("my assistant " + toolQueryOptions, config.ConnectionStringName,
                "Be helpful")
            {
                SampleObject = JsonConvert.SerializeObject(new Reply(Answer: "The answer to the user's question")),
                Queries = [new AiAgentToolQuery
                {
                    Name = "GetUserDetails",
                    Description = "Gets the current user details",
                    ParametersSampleObject = "{}",
                    Query = "from Users where Username = $username select FirstName, LastName",
                    Options = toolQueryOptions
                }],
                Parameters = [new AiAgentParameter("username", "The username of the current user")]
            };
            return aiAgentConfiguration;
        }

        var identifierAddInitialContext = (await store.AI.CreateAgentAsync(
            CreateAgent(AiAgentToolQueryOptions.AddToInitialContext), AiAgentBasics.OutputSchema.Instance)
            ).Identifier;
        
        var identifierAllowModelQueries = (await store.AI.CreateAgentAsync(
                CreateAgent(AiAgentToolQueryOptions.AllowModelQueries), AiAgentBasics.OutputSchema.Instance)
            ).Identifier;


        await VerifyCall(identifierAllowModelQueries, 2);
        await VerifyCall(identifierAddInitialContext, 1);

        async Task VerifyCall(string identifier, int expectedCallCount)
        {
            var chat = store.AI.Conversation(identifier, "chats/",
                new AiConversationCreationOptions()
                    .AddParameter("username", "ayende"));

            chat.SetUserPrompt("What is my name?");
            var result = await chat.RunAsync<Reply>();
            Assert.Contains("Oren", result.Answer.Answer);
            Assert.Contains("Eini", result.Answer.Answer);

            using (var session = store.OpenAsyncSession())
            {
                var conversation = await session.LoadAsync<JObject>(chat.Id);
                // count all the messages with "usages" on them - indicating that this is a model reply
                // so we can check how many times we talked to the model
                int countOfCalls = conversation.Value<JArray>("Messages").OfType<JObject>()
                    .Count(x=>x.ContainsKey("usage"));
                Assert.Equal(expectedCallCount, countOfCalls);
            }
        }
    }
}
