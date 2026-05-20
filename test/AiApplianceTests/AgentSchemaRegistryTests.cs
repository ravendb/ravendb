using FastTests;
using Raven.AiAppliance.Agents;
using Raven.AiAppliance.Schema;
using Raven.Client.Documents.AI;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class AgentSchemaRegistryTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Core)]
    public void Empty_registry_resolves_to_empty_All()
    {
        var registry = new AgentSchemaRegistry([]);
        Assert.Empty(registry.All);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void TryGet_returns_false_for_unknown_identifier()
    {
        var registry = new AgentSchemaRegistry([new FakeSchema("a")]);
        Assert.False(registry.TryGet("missing", out var s));
        Assert.Null(s);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void TryGet_resolves_registered_schemas_case_insensitively()
    {
        var fake = new FakeSchema("Demo-Agent");
        var registry = new AgentSchemaRegistry([fake]);
        Assert.True(registry.TryGet("demo-agent", out var s));
        Assert.Same(fake, s);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void Require_throws_when_identifier_is_missing()
    {
        var registry = new AgentSchemaRegistry([new FakeSchema("a")]);
        Assert.Throws<KeyNotFoundException>(() => registry.Require("b"));
    }

    [RavenFact(RavenTestCategory.Core)]
    public void Duplicate_identifier_in_DI_is_a_startup_error()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            new AgentSchemaRegistry([new FakeSchema("dup"), new FakeSchema("dup")]));
        Assert.Contains("'dup'", ex.Message);
    }

    private sealed class FakeSchema(string id) : IAgentSchema
    {
        public string Identifier => id;
        public string DisplayName => id;
        public string SystemPrompt => "";
        public Type AnswerType => typeof(object);
        public object AnswerSample => new();
        public IReadOnlyList<AgentParameter> Parameters => [];
        public IReadOnlyList<AgentToolQuery> Queries => [];
        public IReadOnlyList<AgentFanoutIndex> FanoutIndexes => [];
        public Task<object> RunConversationAsync(IAiConversationOperations conversation, Func<string, ValueTask> onChunk, CancellationToken ct) =>
            throw new NotSupportedException();
    }
}
