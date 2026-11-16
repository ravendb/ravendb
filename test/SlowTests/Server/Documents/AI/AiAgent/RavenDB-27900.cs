using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

// An empty streamed answer used to be stored and then re-sent with the history, breaking every later turn.
public class RavenDB_27900(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string ConnectionStringName = "empty-completion-connection";
    private const string UserPrompt = "What is 2+2? Reply with just the number.";
    private const string SystemPrompt = "Answer the user's question briefly.";

    [RavenFact(RavenTestCategory.Ai)]
    public async Task PlainText_StreamCutByTheTokenLimit_ThrowsTooManyTokens()
    {
        using var store = GetDocumentStore();

        // the budget was spent before a single content delta was produced
        var (_, error) = await StreamAsync(store, "chats/cut-by-length", Wire.Stream(Wire.FinishChunk("length")));

        Assert.True(AiAgentTest.HasException<TooManyTokensException>(error), $"expected TooManyTokensException, got: {error}");

        // the failure classifier reads this marker together with the type
        Assert.Contains("finish_reason='length'", error.ToString());
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task PlainText_StreamWithNoContent_PersistsNoAssistantMessage()
    {
        const string conversationId = "chats/survives-an-empty-answer";

        using var store = GetDocumentStore();

        var (_, error) = await StreamAsync(store, conversationId, Wire.Stream(Wire.FinishChunk("stop")));
        Assert.True(AiAgentTest.HasException<UnexpectedResponseException>(error), $"expected UnexpectedResponseException, got: {error}");
        Assert.Contains("No response content", error.ToString());

        // nothing from the failed turn is left behind
        (_, error) = await StreamAsync(store, conversationId, Wire.Stream(Wire.ContentDelta("4"), Wire.FinishChunk("stop")));
        Assert.Null(error);

        // Full: Simple hides an assistant message whose content is null
        var messages = await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions
        {
            ConversationId = conversationId,
            DetailLevel = AiConversationDetailLevel.Full
        });

        var assistant = messages.Messages.Where(m => m.Role == AiMessageRole.Assistant).ToList();

        Assert.Single(assistant);
        Assert.Equal("4", assistant[0].Content);
    }

    private async Task<(string streamed, Exception error)> StreamAsync(IDocumentStore store, string conversationId, string sse)
    {
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(FakeConnection()));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        return await AiAgentTest.StreamAsync(Server.ServerStore, database, FakeConnection(), InjectedResponse.Sse(sse), string.Empty,
            handler => handler.Initialize(Agent(), conversationId, new RequestBody
            {
                Parameters = null,
                CreationOptions = new AiConversationCreationOptions { MaxModelIterationsPerCall = 16 },
                UserPrompt = UserPrompt,
                OutputOptions = new AiServerOutputOptions { NoSchema = true }
            }, changeVector: null));
    }

    private static AiAgentConfiguration Agent() =>
        new("empty-completion-agent", ConnectionStringName, SystemPrompt) { Identifier = "empty-completion-agent" };

    private static AiConnectionString FakeConnection() => AiAgentTest.FakeConnection(ConnectionStringName);

}
