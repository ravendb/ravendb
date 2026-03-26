using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class ArtificalActions(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanAddArtificialActionsToConversation(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));


        var agent = new AiAgentConfiguration("shopping-assistant", config.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
        agent.Parameters.Add(new AiAgentParameter("company", "The company ID"));
        agent.Identifier = "shopping-assistant";
        agent.SampleObject = JsonConvert.SerializeObject(new ModelAnswer(true, "reason for the true/false recommendation"));

        var createResult = await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance);
        var chat = store.AI.Conversation(
            createResult.Identifier,
            "chats/",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

        chat.AddArtificialActionWithResponse("GetUserAllergies", "Gluten, Lactose");
        chat.SetUserPrompt("Should I get regular cheese?");
        var r = await chat.RunAsync<ModelAnswer>(CancellationToken.None);

        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.False(r.Answer.Recommend, r.Answer.Reason);
    }

    private record ModelAnswer(bool Recommend, string Reason);
}
