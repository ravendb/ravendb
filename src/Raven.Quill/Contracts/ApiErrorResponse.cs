namespace Raven.Quill.Contracts;

public sealed record ApiErrorResponse(string? Error = null, string[]? Errors = null, string? Code = null);
