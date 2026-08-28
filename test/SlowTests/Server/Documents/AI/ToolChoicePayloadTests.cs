using System;
using System.Linq;
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

namespace SlowTests.Server.Documents.AI;

public class ToolChoicePayloadTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Schema
    {
        public string Answer = "Answer to the user question";
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task WhenToolIterationsAreExhausted_TheModelCannotCallTools(Options options, GenAiConfiguration config)
    {
        // With no tool iterations left, the model must not call a tool even though it is told to.
        // Providers that honor 'tool_choice: "none"' obey it; for the rest the tools are not sent at all.
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("tool-choice-agent", config.ConnectionStringName,
            "Answer with a single short sentence.")
        {
            Identifier = "tool-choice-agent",
            Actions =
            [
                new AiAgentToolAction
                {
                    Name = "MyTool",
                    Description = "Returns an integer",
                    ParametersSampleObject = "{}"
                }
            ],
            MaxModelIterationsPerCall = 0 // no tool iterations at all, so the very first request has tools disabled
        };

        await store.AI.CreateAgentAsync(agent, new Schema());

        var chat = store.AI.Conversation(agent.Identifier, "chats/", creationOptions: null, debug: true);

        var toolWasCalled = false;
        chat.Handle<object>("MyTool", _ =>
        {
            toolWasCalled = true;
            return "42";
        });

        chat.SetUserPrompt("Call the 'MyTool' tool.");

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var result = await chat.RunAsync<Schema>(cts.Token);

        Assert.False(toolWasCalled, "The model must not be able to call a tool once the tool iterations are exhausted.");
        Assert.Equal(AiConversationResult.Done, result.Status);

        // the request the server actually sent to the provider
        using var session = store.OpenSession();
        var traces = session.Advanced.LoadStartingWith<BlittableJsonReaderObject>($"{chat.Id}/{AiDebugTrace.TraceSegment}/", pageSize: 1024);
        var requests = traces.Select(t =>
        {
            Assert.True(t.TryGet(nameof(AiDebugTrace.RequestBody), out string requestBody));
            return requestBody;
        }).ToArray();

        Assert.NotEmpty(requests);

        foreach (var request in requests)
        {
            if (config.AiConnectorType == AiConnectorType.Ollama)
            {
                // Ollama ignores 'tool_choice', so the tools themselves must not be offered.
                Assert.DoesNotContain("\"tools\"", request);
                Assert.DoesNotContain("\"tool_choice\"", request);
            }
            else
            {
                Assert.Contains("\"tools\"", request);
                Assert.Contains("\"tool_choice\":\"none\"", request);
            }
        }
    }
}
