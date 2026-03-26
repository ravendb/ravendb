using System;
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

public class RavenDB_25307 : RavenTestBase
{
    public RavenDB_25307(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CallingAddActionResponseFromHandle_ShouldThrow(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agentConfig = new AiAgentConfiguration("alert-tester", config.ConnectionStringName, "You are a test agent. Your only purpose is to call the 'log-user-query' tool, no matter what the user says.")
        {
            Actions =
            [
                new AiAgentToolAction { Name = "log-user-query", Description = "A tool that logs a query", ParametersSampleObject = "{}" }
            ],
            SampleObject = JsonConvert.SerializeObject(new { answer = "string" })
        };
        var agent = await store.AI.CreateAgentAsync(agentConfig);

        var conversation = store.AI.Conversation(agent.Identifier, "chats/", creationOptions: null);

        conversation.Handle("log-user-query", (AiAgentActionRequest request, dynamic data) =>
        {
            // calling AddActionResponse inside Handle is now blocked --> this should throw 
            conversation.AddActionResponse(request.ToolId, "logged");
            return "ok";
        });

        conversation.SetUserPrompt("Please run the tool.");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await conversation.RunAsync<object>());
        Assert.Contains("Each tool call must have exactly one response", ex.Message);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Handle_TaskOfTResult_DoesNotBindToObjectOverload(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agentConfig = new AiAgentConfiguration("tester", config.ConnectionStringName, "You are a test agent. Your only purpose is to call the 'RecentOrder' tool, no matter what the user says.")
        {
            Actions =
            [
                new AiAgentToolAction
                {
                    Name = "RecentOrder", 
                    Description = "test tool", 
                    ParametersSampleObject = "{}"
                }
            ],
            SampleObject = JsonConvert.SerializeObject(new { answer = "string" })
        };

        var agent = await store.AI.CreateAgentAsync(agentConfig);
        var chat = store.AI.Conversation(agent.Identifier, "chats/", creationOptions: null);

        // should be resolved to the async overload of Handle
        chat.Handle("RecentOrder", (object query) =>
        {
            return Task.FromResult("done");
        }, AiHandleErrorStrategy.RaiseImmediately);

        chat.SetUserPrompt("Please run the tool.");

        // should not throw
        await chat.RunAsync<object>();
    }
}
