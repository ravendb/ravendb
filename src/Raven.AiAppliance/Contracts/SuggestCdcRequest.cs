namespace Raven.AiAppliance.Contracts;

/// <summary>Dashboard request for an AI-suggested CDC mapping. The admin's intent in prose.</summary>
public sealed record SuggestCdcRequest(string IntentPrompt);
