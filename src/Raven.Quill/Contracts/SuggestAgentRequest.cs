namespace Raven.Quill.Contracts;

/// <summary>
/// Dashboard request for an AI-suggested agent. <see cref="Mode"/> is <c>from-data</c>
/// (derives 1-3 candidates from the app's CDC config and sample docs) or <c>from-prompt</c>
/// (produces a single candidate from the intent). <see cref="IntentPrompt"/> is optional
/// for <c>from-data</c>.
/// </summary>
public sealed record SuggestAgentRequest(string? IntentPrompt, string Mode);
