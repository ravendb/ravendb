namespace Raven.Quill.Contracts;

/// <summary>Body of <c>POST /api/auth/login</c>: the operator API key to validate.</summary>
public sealed record LoginRequest(string ApiKey);
