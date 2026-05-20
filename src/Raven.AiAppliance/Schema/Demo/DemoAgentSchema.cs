using Raven.AiAppliance.Agents;
using Raven.Client.Documents.AI;

namespace Raven.AiAppliance.Schema.Demo;

/// Placeholder schema for the 8-week demo. T-3's wizard will replace this with
/// schemas derived from real CDC-discovered tables; the shape mirrors the
/// nopcommerce-demo template so the end-to-end registrar path is exercised.
internal sealed class DemoAgentSchema : IAgentSchema
{
    public string Identifier => "demo-agent";
    public string DisplayName => "Demo Agent";

    public string SystemPrompt => """
        You are a placeholder agent shipped with the AI Appliance demo image.
        Until the operator runs the per-app provisioning wizard, you have no
        domain knowledge — politely tell the user the appliance is in demo mode
        and point them at the dashboard.
        """;

    public Type AnswerType => typeof(DemoAgentAnswer);
    public object AnswerSample => DemoAgentAnswer.Sample;

    public IReadOnlyList<AgentParameter> Parameters { get; } = [];
    public IReadOnlyList<AgentToolQuery> Queries { get; } = [];
    public IReadOnlyList<AgentFanoutIndex> FanoutIndexes { get; } = [];

    public async Task<object> RunConversationAsync(
        IAiConversationOperations conversation,
        Func<string, ValueTask> onChunk,
        CancellationToken ct)
    {
        var answer = await conversation.StreamAsync<DemoAgentAnswer>(
            a => a.Reply,
            async chunk => await onChunk(chunk),
            ct);
        return answer.Answer;
    }
}
