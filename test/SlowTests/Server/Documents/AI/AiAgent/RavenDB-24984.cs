using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_24984 : RavenTestBase
{
    public RavenDB_24984(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ShouldStopUsingToolsWhenMaxModelIterationsPerCallIsReached(Options options, GenAiConfiguration config)
    {
        const int maxModelIterationsPerCall = 5;

        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new CreateSampleDataOperation());

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        const string systemPrompt = "your whole purpose is to run the tool called 'MyTool'";
        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName, systemPrompt);
        agent.Identifier = "shopping-assistant";

        agent.Actions =
        [
            new AiAgentToolAction
                {
                    Name = "MyTool",
                    Description =  "returns an integer",
                    ParametersSampleObject = "{}"
                }
        ];

        agent.MaxModelIterationsPerCall = maxModelIterationsPerCall;

        await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance);
        var chat = store.AI.Conversation(
            agent.Identifier,
            "chats/",
            creationOptions: null);

        int lastNumber = 1;
        chat.Handle<object>("MyTool", _ => lastNumber++.ToString());

        chat.SetUserPrompt("call the 'MyTool' tool until it returns the string '10' (or greater)");

        var result = await chat.RunAsync<AiAgentBasics.OutputSchema>(CancellationToken.None);

        Assert.Equal(AiConversationResult.Done, result.Status);

        var lastToolResult = lastNumber - 1;
        Assert.Equal(lastToolResult.ToString(), result.Answer.Answer);
        Assert.Equal(maxModelIterationsPerCall, lastToolResult);

        using (var session = store.OpenSession())
        {
            var conversationDoc = session.Load<BlittableJsonReaderObject>(chat.Id);
            Assert.True(conversationDoc.TryGet(nameof(ConversationDocument.NumberOfRepeatedToolCalls), out int numOfRepeatedToolCalls));
            Assert.Equal(0, numOfRepeatedToolCalls); // should be reset after successful completion
        }
    }
}
