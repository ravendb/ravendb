namespace Raven.AiAppliance.Contracts;

public sealed record ApiErrorResponse(string? Error = null, string[]? Errors = null);
