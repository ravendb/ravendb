namespace Raven.AiAppliance.Contracts;

/// <param name="Code">Optional machine-readable code for clients that branch
/// on the failure kind (embed auth: <c>conversation_unknown</c> /
/// <c>conversation_expired</c> / <c>origin_forbidden</c>). Omitted (null)
/// everywhere else — additive, existing consumers unaffected.</param>
public sealed record ApiErrorResponse(string? Error = null, string[]? Errors = null, string? Code = null);
