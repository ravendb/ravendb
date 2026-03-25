using System;
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

        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
            "Your whole purpose is to repeatedly call the tool 'MyTool'. " +
            "After each call, you receive a numeric value. " +

            "If you stop for any reason (including reaching iteration limits), " +
            "you MUST return the last tool value as the Answer. " +

            "Answer must be exactly the last tool value as plain text, with no additional text.")
        {
            Identifier = "shopping-assistant",
            Actions =
            [
                new AiAgentToolAction
                {
                    Name = "MyTool",
                    Description =  "returns an integer",
                    ParametersSampleObject = "{}"
                }
            ],
            MaxModelIterationsPerCall = maxModelIterationsPerCall,
        };

        var schemaJson = """
                         {
                           "name": "ZnYyQ3BUOGZYUXY3YXJqQm1uTGVOVytLeXdxQW82L0k5T0R4VGs3cFJzZz0",
                           "strict": true,
                           "schema": {
                             "type": "object",
                             "properties": {
                               "Answer": {
                                 "type": "string",
                                 "description": "Answer to the user question"
                               },
                               "Relevant": {
                                 "type": "boolean"
                               },
                               "RelevantOrdersId": {
                                 "type": "array",
                                 "items": {
                                   "type": "string",
                                   "description": "The order ids relevant to the query or response"
                                 }
                               },
                               "MatchingProductsId": {
                                 "type": "array",
                                 "items": {
                                   "type": "string",
                                   "description": "All the product ids referenced either by the user or the system"
                                 }
                               }
                             },
                             "parallel_tool_calls" : false,
                             "required": [
                               "Answer",
                               "Relevant",
                               "RelevantOrdersId",
                               "MatchingProductsId"
                             ],
                             "additionalProperties": false
                           }
                         }
                         """;
        agent.OutputSchema = schemaJson;
        await store.AI.CreateAgentAsync(agent);

        var chat = store.AI.Conversation(
            agent.Identifier,
            "chats/",
            creationOptions: null);

        int lastNumber = 1;
        chat.Handle<object>("MyTool", _ => lastNumber++.ToString());

        chat.SetUserPrompt("call the 'MyTool' tool until it returns a value >= 10");

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var result = await chat.RunAsync<AiAgentBasics.OutputSchema>(cts.Token);

        Assert.Equal(AiConversationResult.Done, result.Status);

        var lastToolResult = lastNumber - 1;
        Assert.Equal(lastToolResult.ToString(), result.Answer.Answer);
        Assert.Equal(maxModelIterationsPerCall, lastToolResult);

        using (var session = store.OpenSession())
        {
            var conversationDoc = session.Load<BlittableJsonReaderObject>(chat.Id);
            Assert.True(conversationDoc.TryGet(nameof(ConversationDocument.RemainingToolIterations), out int remainingToolIterations));
            Assert.Equal(maxModelIterationsPerCall, remainingToolIterations); // should be reset after successful completion
        }
    }
}
