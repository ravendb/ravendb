using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_24883 : RavenTestBase
{
    public RavenDB_24883(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ReceiveWillBeCalledButNotCloseTheAction(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = AiAgentClientApiHandleActionCalls.BuildAgent(config.ConnectionStringName);
        agent.Actions.RemoveAll(a => a.Name == AiAgentClientApiHandleActionCalls.ProductSearch);

        var r  = await store.AI.CreateAgentAsync(agent, new 
        {
            Answer = "the answer"
        });
        var chat = store.AI.Conversation(
            r.Identifier,
            "chats/123",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

        var recentOrderCalled = false;
        string toolId = "";
        chat.Receive(AiAgentClientApiHandleActionCalls.RecentOrder, (AiAgentActionRequest req,object query) =>
        {
            toolId = req.ToolId;
            recentOrderCalled = true;
        });

        chat.SetUserPrompt("fetch my recent orders");
        var run = await chat.RunAsync<object>();
        Assert.Equal(run.Status, AiConversationResult.ActionRequired);
        Assert.Null(run.Answer);
        Assert.True(recentOrderCalled);
        
        chat.AddActionResponse(toolId, "my recent orders: Pizza, Burger");
        recentOrderCalled = false;  
        run = await chat.RunAsync<object>();
        Assert.Equal(AiConversationResult.Done, run.Status);
        Assert.NotNull(run.Answer);
        Assert.False(recentOrderCalled);
        
    }

}
