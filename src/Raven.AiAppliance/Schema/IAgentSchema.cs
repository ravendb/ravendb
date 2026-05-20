using Raven.AiAppliance.Agents;
using Raven.Client.Documents.AI;

namespace Raven.AiAppliance.Schema;

public interface IAgentSchema
{
    string Identifier { get; }
    string DisplayName { get; }
    string SystemPrompt { get; }
    Type AnswerType { get; }
    object AnswerSample { get; }
    IReadOnlyList<AgentParameter> Parameters { get; }
    IReadOnlyList<AgentToolQuery> Queries { get; }
    IReadOnlyList<AgentFanoutIndex> FanoutIndexes { get; }

    /// Runs a single conversation turn. The schema owns the concrete answer
    /// type so callers stay agnostic of T. Implementations should pump chunks
    /// of the "Reply" field through <paramref name="onChunk"/> and return the
    /// full answer object for the `done` envelope.
    Task<object> RunConversationAsync(
        IAiConversationOperations conversation,
        Func<string, ValueTask> onChunk,
        CancellationToken ct);
}
