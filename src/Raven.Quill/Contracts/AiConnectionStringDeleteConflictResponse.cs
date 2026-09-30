namespace Raven.Quill.Contracts;

public sealed record AiConnectionStringDeleteConflictResponse(string Error, string[] ReferencingAgentIds);
