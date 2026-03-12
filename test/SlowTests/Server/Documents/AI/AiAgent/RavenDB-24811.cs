using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_24811(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanStreamResults(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("my assistant", config.ConnectionStringName,
            "Be helpful");

        var identifier = (await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(identifier, "chats/",
            new AiConversationCreationOptions());

        chat.SetUserPrompt("Give me 15 real cities names, one per line");
        var sb = new StringBuilder();
        var result = await chat.StreamAsync<AiAgentBasics.OutputSchema>( a=>a.Answer, s =>
        { 
            sb.Append(s);
            return Task.CompletedTask;
        }, CancellationToken.None);
        
        Assert.Equal(result.Answer.Answer, sb.ToString());
    }
    
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanStreamResults_WithTools(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("my assistant", config.ConnectionStringName,
            "Always call the 'whoami' tool before answering any user prompts")
        {
            Actions = [
            new AiAgentToolAction
            {
                Name = "whoami",
                ParametersSampleObject = "{}",
                Description = "Describe who this user is, preferences, etc"
            }]
        };

        var identifier = (await store.AI.CreateAgentAsync(agent, AiAgentBasics.OutputSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(identifier, "chats/",
            new AiConversationCreationOptions());
        bool wasCalled = false;
        AiAgentActionRequest actionRequest = null;
        chat.Receive<object>("whoami", (a, args) =>
        {
            actionRequest = a;
            wasCalled = true;
        });

        chat.SetUserPrompt("Give me 15 real cities names, one per line");
        var sb = new StringBuilder();
        var result = await chat.StreamAsync<AiAgentBasics.OutputSchema>(x=>x.Answer, s =>
        {
            sb.Append(s);
            return Task.CompletedTask;
        }, CancellationToken.None);
        Assert.Equal(AiConversationResult.ActionRequired,result.Status);
        Assert.True(wasCalled);
        Assert.Empty(sb.ToString());
        chat.AddActionResponse(actionRequest.ToolId, "I'm batman");
        
        result = await chat.StreamAsync<AiAgentBasics.OutputSchema>("Answer", s =>
        {
            sb.Append(s);
            return Task.CompletedTask;
        }, CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, result.Status);
        Assert.Equal(result.Answer.Answer, sb.ToString());
    }
}
