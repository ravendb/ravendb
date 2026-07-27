namespace Raven.Quill.Contracts;

public sealed record SuggestCdcRequest(string? IntentPrompt, string Slug = "");
