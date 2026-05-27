namespace Raven.AiAppliance.Contracts;

public sealed record AiConnectionStringDeleteConflictResponse(string Error, string[] ReferencingAgentIds);
