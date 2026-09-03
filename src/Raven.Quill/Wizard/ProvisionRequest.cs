namespace Raven.Quill.Wizard;

public sealed record ProvisionRequest(string AppName, string? Slug = null);
