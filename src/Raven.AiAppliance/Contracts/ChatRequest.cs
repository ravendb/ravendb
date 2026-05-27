using System.Collections.Generic;

namespace Raven.AiAppliance.Contracts;

public sealed record ChatRequest(
    string AgentId,
    string Prompt,
    string? ConversationId,
    Dictionary<string, string>? Parameters);
