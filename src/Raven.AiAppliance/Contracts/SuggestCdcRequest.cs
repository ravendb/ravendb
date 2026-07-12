namespace Raven.AiAppliance.Contracts;

/// <summary>Dashboard request for an AI-suggested CDC mapping. <see cref="IntentPrompt"/> is the
/// admin's intent in prose; it is optional — a blank value falls back to a premade default prompt.</summary>
public sealed record SuggestCdcRequest(string? IntentPrompt);
