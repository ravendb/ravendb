using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

// Text between tool-call chunks used to close the call, so a call split across chunks arrived as two - half
// committed, the rest made into a second call. The boundary is now the id; RavenDB-26944's
// ToolCalls_SplitByReasoningDeltas_AreNotMergedIntoOne pins the converse, two calls that share an index.
public class RavenDB_27950(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string UserPrompt = "What is 2+2? Reply with just the number.";
    private const string SystemPrompt = "Answer the user's question.";
    private const string ToolName = "MyTool";

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ContentBetweenTwoFragmentsDoesNotSplitTheCall()
    {
        var sse = Wire.Stream(
            Wire.ToolCallDelta(index: 0, id: "call_a", name: ToolName, arguments: "{\"value\":"),
            Wire.ContentDelta("Let me look that up."),
            Wire.ToolCallArgumentsDelta(index: 0, arguments: "42}"),
            Wire.FinishChunk("tool_calls"));

        var calls = await StreamAndReadToolCallsAsync("chats/split-by-content", sse);

        var call = Assert.Single(calls);
        Assert.Equal("call_a", call.Id);
        Assert.Equal("{\"value\":42}", call.Arguments);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ReasoningBetweenTwoFragmentsDoesNotSplitTheCall()
    {
        // The twin of the test above: the two deltas were separate resets, so each needs its own case.
        var sse = Wire.Stream(
            Wire.ToolCallDelta(index: 0, id: "call_a", name: ToolName, arguments: "{\"value\":"),
            Wire.ReasoningDelta("Thinking about which value to use."),
            Wire.ToolCallArgumentsDelta(index: 0, arguments: "42}"),
            Wire.FinishChunk("tool_calls"));

        var calls = await StreamAndReadToolCallsAsync("chats/split-by-reasoning", sse);

        var call = Assert.Single(calls);
        Assert.Equal("call_a", call.Id);
        Assert.Equal("{\"value\":42}", call.Arguments);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task AnIdRepeatedOnEveryFragmentIsOneCall()
    {
        // Gateways repeat the id on every chunk. Only a *different* id is a different call - the id the
        // repeat accumulates into is left as it was, that is not this ticket.
        var sse = Wire.Stream(
            Wire.ToolCallDelta(index: 0, id: "call_a", name: ToolName, arguments: "{\"value\":"),
            Wire.ToolCallDelta(index: 0, id: "call_a", name: "", arguments: "42}"),
            Wire.FinishChunk("tool_calls"));

        var calls = await StreamAndReadToolCallsAsync("chats/repeated-id", sse);

        var call = Assert.Single(calls);
        Assert.Equal(ToolName, call.Name);
        Assert.Equal("{\"value\":42}", call.Arguments);
    }

    private async Task<List<ToolCall>> StreamAndReadToolCallsAsync(string conversationId, string sse)
    {
        using var store = GetDocumentStore();
        await StreamAsync(store, conversationId, sse);

        return ReadStoredToolCalls(store, conversationId);
    }

    private record ToolCall(string Id, string Name, string Arguments);

    // The tool calls as they were stored - the history re-sent on every later turn. Read out as strings
    // inside the session: the blittable does not outlive it.
    private static List<ToolCall> ReadStoredToolCalls(IDocumentStore store, string conversationId)
    {
        using var session = store.OpenSession();
        var document = session.Load<BlittableJsonReaderObject>(conversationId);
        Assert.NotNull(document);

        Assert.True(document.TryGet("Messages", out BlittableJsonReaderArray messages), "no Messages in the conversation document");

        foreach (BlittableJsonReaderObject message in messages)
        {
            if (message.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray calls) == false ||
                calls.Length == 0)
                continue;

            var toolCalls = new List<ToolCall>();
            foreach (BlittableJsonReaderObject call in calls)
            {
                call.TryGet(ChatCompletionClient.Constants.ResponseFields.Id, out string id);
                call.TryGet(ChatCompletionClient.Constants.ResponseFields.Function, out BlittableJsonReaderObject function);
                function.TryGet(ChatCompletionClient.Constants.ResponseFields.Name, out string name);
                function.TryGet(ChatCompletionClient.Constants.ResponseFields.Arguments, out string arguments);
                toolCalls.Add(new ToolCall(id, name, arguments));
            }

            return toolCalls;
        }

        throw new InvalidOperationException($"no message with tool_calls in: {document}");
    }

    private async Task StreamAsync(IDocumentStore store, string conversationId, string sse)
    {
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(FakeConnection()));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = new InjectingConversationHandler(Server.ServerStore, database, FakeConnection(),
                InjectedResponse.Sse(sse)) { Authentication = null };

            handler.Initialize(Agent(), conversationId, new RequestBody
            {
                Parameters = null,
                CreationOptions = new AiConversationCreationOptions { MaxModelIterationsPerCall = 16 },
                UserPrompt = UserPrompt,
                OutputOptions = new AiServerOutputOptions { NoSchema = true }
            }, changeVector: null);

            using var clientStream = new MemoryStream();
            await handler.HandleStreamingRequestAsync(context, clientStream, string.Empty, CancellationToken.None);
        }
    }

    private static AiAgentConfiguration Agent() =>
        new("ravendb-27950-agent", FakeConnectionStringName, SystemPrompt)
        {
            Identifier = "ravendb-27950-agent",
            Actions = [new AiAgentToolAction { Name = ToolName, Description = "Returns an integer", ParametersSampleObject = "{}" }]
        };

    private const string FakeConnectionStringName = "ravendb-27950-connection";

    private static AiConnectionString FakeConnection() => new()
    {
        Name = FakeConnectionStringName,
        Identifier = FakeConnectionStringName,
        ModelType = AiModelType.Chat,
        OpenAiSettings = new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o")
    };
}
