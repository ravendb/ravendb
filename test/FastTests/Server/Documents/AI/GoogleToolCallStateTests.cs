using System.Linq;
using Raven.Server.Documents.AI.Settings;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.AI;

public class GoogleToolCallStateTests : RavenTestBase
{
    public GoogleToolCallStateTests(ITestOutputHelper output) : base(output)
    {
    }

    private const string FullChunk = @"{
        ""id"": ""call_abc123"",
        ""type"": ""function"",
        ""function"": { ""name"": ""searchProduct"", ""arguments"": ""{\""id\"": 1}"" },
        ""extra_content"": { ""google"": { ""thought_signature"": ""Ciwx8vK2"" } }
    }";

    // What Google streams for every call after the first one in a response.
    private const string ChunkWithoutExtraContent = @"{
        ""id"": ""call_abc123"",
        ""type"": ""function"",
        ""function"": { ""name"": ""searchProduct"", ""arguments"": ""{\""id\"": 1}"" }
    }";

    [RavenFact(RavenTestCategory.Ai)]
    public void MissingExtraContentIsAcceptedAndTheCallSurvives()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var state = new GoogleToolCallState();
        state.Merge(Chunk(context, ChunkWithoutExtraContent));
        state.AddAndReset();

        var call = Assert.Single(state.GetAllToolCalls());
        Assert.Equal("call_abc123", call.Id);
        Assert.Equal("searchProduct", call.Name);
        Assert.Equal(@"{""id"": 1}", call.Arguments);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData(@"{ ""id"": ""c1"", ""type"": ""function"", ""function"": { ""name"": ""f"", ""arguments"": ""{}"" } }")]
    [InlineData(@"{ ""id"": ""c1"", ""type"": ""function"", ""function"": { ""name"": ""f"", ""arguments"": ""{}"" }, ""extra_content"": {} }")]
    [InlineData(@"{ ""id"": ""c1"", ""type"": ""function"", ""function"": { ""name"": ""f"", ""arguments"": ""{}"" }, ""extra_content"": { ""google"": {} } }")]
    [InlineData(@"{ ""id"": ""c1"", ""type"": ""function"", ""function"": { ""name"": ""f"", ""arguments"": ""{}"" }, ""extra_content"": null }")]
    [InlineData(@"{ ""id"": ""c1"", ""type"": ""function"", ""function"": { ""name"": ""f"", ""arguments"": ""{}"" }, ""extra_content"": { ""google"": null } }")]
    [InlineData(@"{ ""id"": ""c1"", ""type"": ""function"", ""function"": { ""name"": ""f"", ""arguments"": ""{}"" }, ""extra_content"": { ""google"": { ""thought_signature"": null } } }")]
    [InlineData(@"{ ""id"": ""c1"", ""type"": ""function"", ""function"": { ""name"": ""f"", ""arguments"": ""{}"" }, ""extra_content"": [] }")]
    [InlineData(@"{ ""id"": ""c1"", ""type"": ""function"", ""function"": { ""name"": ""f"", ""arguments"": ""{}"" }, ""extra_content"": ""x"" }")]
    public void EveryLevelOfExtraContentIsOptional(string json)
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var state = new GoogleToolCallState();
        state.Merge(Chunk(context, json));
        state.AddAndReset();

        Assert.Equal("f", Assert.Single(state.GetAllToolCalls()).Name);
        Assert.Null(ExtraContentOf(context, json));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void ThoughtSignatureIsEchoedBackOnlyWhenGoogleSentOne()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        Assert.Null(ExtraContentOf(context, ChunkWithoutExtraContent));
        Assert.NotNull(ExtraContentOf(context, FullChunk));
    }

    // Measured against gemini-3.8-flash on v1beta/openai/chat/completions: when the model emits
    // several tool calls in one response, Google attaches the thought signature to the first one only.
    [RavenFact(RavenTestCategory.Ai)]
    public void OnlyTheFirstCallOfAResponseCarriesAThoughtSignature()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var state = new GoogleToolCallState();
        state.Merge(Chunk(context, @"{ ""id"": ""call_675880"", ""type"": ""function"",
            ""function"": { ""name"": ""searchProduct"", ""arguments"": ""{\""id\"":17}"" },
            ""extra_content"": { ""google"": { ""thought_signature"": ""Eu4CCusCAWkUfRMQ"" } } }"));
        state.Merge(Chunk(context, @"{ ""id"": ""call_675881"", ""type"": ""function"",
            ""function"": { ""name"": ""searchProduct"", ""arguments"": ""{\""id\"":42}"" } }"));
        state.AddAndReset();

        var calls = state.GetAllToolCalls();
        Assert.Equal(2, calls.Count);
        Assert.Equal(@"{""id"":17}", calls.Single(c => c.Id == "call_675880").Arguments);
        Assert.Equal(@"{""id"":42}", calls.Single(c => c.Id == "call_675881").Arguments);

        Assert.True(state.TryGetToolCallsForMessage(out var toolCalls));
        var message = context.ReadObject(new DynamicJsonValue { ["tool_calls"] = toolCalls }, "message");
        Assert.True(message.TryGet("tool_calls", out BlittableJsonReaderArray rendered));

        Assert.Single(rendered.Cast<BlittableJsonReaderObject>()
            .Where(call => call.TryGet("extra_content", out BlittableJsonReaderObject _)));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TypeIsNotRequired()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        var state = new GoogleToolCallState();
        state.Merge(Chunk(context, @"{ ""id"": ""c1"", ""function"": { ""name"": ""f"", ""arguments"": ""{}"" } }"));
        state.AddAndReset();

        Assert.Equal("f", Assert.Single(state.GetAllToolCalls()).Name);

        Assert.True(state.TryGetToolCallsForMessage(out var toolCalls));
        var message = context.ReadObject(new DynamicJsonValue { ["tool_calls"] = toolCalls }, "message");
        Assert.True(message.TryGet("tool_calls", out BlittableJsonReaderArray rendered));
        Assert.True(((BlittableJsonReaderObject)rendered[0]).TryGet("type", out string type));
        Assert.Equal("function", type);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void TheFieldsTheCallCannotBeMadeWithoutStillThrow()
    {
        using var context = JsonOperationContext.ShortTermSingleUse();

        AssertThrows(@"{ ""type"": ""function"", ""function"": { ""name"": ""f"", ""arguments"": ""{}"" } }");    // no id
        AssertThrows(@"{ ""id"": ""c1"" }");                                                                      // no function
        AssertThrows(@"{ ""id"": ""c1"", ""function"": { ""arguments"": ""{}"" } }");                             // no name
        AssertThrows(@"{ ""id"": ""c1"", ""function"": { ""name"": ""f"" } }");                                   // no arguments

        void AssertThrows(string json)
        {
            var state = new GoogleToolCallState();
            Assert.Throws<System.InvalidOperationException>(() => state.Merge(Chunk(context, json)));
        }
    }

    private static BlittableJsonReaderObject Chunk(JsonOperationContext context, string json)
        => context.Sync.ReadForMemory(json, "tool-call/chunk");

    private static BlittableJsonReaderObject ExtraContentOf(JsonOperationContext context, string chunkJson)
    {
        var state = new GoogleToolCallState();
        state.Merge(Chunk(context, chunkJson));
        state.AddAndReset();

        Assert.True(state.TryGetToolCallsForMessage(out var toolCalls));

        var message = context.ReadObject(new DynamicJsonValue { ["tool_calls"] = toolCalls }, "message");
        Assert.True(message.TryGet("tool_calls", out BlittableJsonReaderArray calls));

        var call = (BlittableJsonReaderObject)calls[0];
        call.TryGet("extra_content", out BlittableJsonReaderObject extraContent);
        return extraContent;
    }
}
