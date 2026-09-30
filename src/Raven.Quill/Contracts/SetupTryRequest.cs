using System.Collections.Generic;
using System.Text.Json;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Contracts;

public sealed record SetupTryRequest(
    string Prompt,
    AiAgentConfiguration Configuration,
    Dictionary<string, SetupTryParameter>? Parameters,
    string? StreamField = null);

public sealed record SetupTryParameter(JsonElement? Value, bool SendToModel);
