namespace Raven.AiAppliance.Contracts;

/// <param name="Code">Optional machine-readable failure code (e.g.
/// <c>origin_forbidden</c>); null elsewhere — additive.</param>
public sealed record ApiErrorResponse(string? Error = null, string[]? Errors = null, string? Code = null);
