namespace Raven.Quill.Contracts;

/// <summary>Response of <c>GET /api/auth/status</c> and <c>POST /api/auth/login</c>: whether the
/// caller is authenticated (a valid session cookie or API-key header).</summary>
public sealed record AuthStatusResponse(bool Authenticated);
