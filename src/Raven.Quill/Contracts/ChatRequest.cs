using System.Collections.Generic;
using System.Text.Json;

namespace Raven.Quill.Contracts;

public sealed record ChatRequest(
    string AgentId,
    string Prompt,
    string ConversationId,
    Dictionary<string, JsonElement>? Parameters);
