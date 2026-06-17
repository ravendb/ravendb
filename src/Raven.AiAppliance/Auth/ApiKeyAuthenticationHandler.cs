using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace Raven.AiAppliance.Auth;

/// <summary>
/// Per-request API-key authentication for the <c>api.*</c> surface: the key travels in an
/// <c>X-Api-Key</c> header (or <c>Authorization: Bearer &lt;key&gt;</c>) and is validated against
/// <see cref="IApiKeyStore"/>. Returns <see cref="AuthenticateResult.NoResult"/> when no key is
/// present so the cookie scheme (the <c>dashboard.*</c> session) can still authenticate the request.
/// </summary>
public sealed class ApiKeyAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder,
    IApiKeyStore keys) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    public const string SchemeName = "ApiKey";
    public const string HeaderName = "X-Api-Key";

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var presented = ExtractKey(Request);
        if (string.IsNullOrEmpty(presented))
            return AuthenticateResult.NoResult();

        if (await keys.ValidateAsync(presented, Context.RequestAborted) == false)
            return AuthenticateResult.Fail("invalid API key");

        var identity = new ClaimsIdentity(SchemeName);
        identity.AddClaim(new Claim(ClaimTypes.Name, "operator"));
        var ticket = new AuthenticationTicket(new ClaimsPrincipal(identity), SchemeName);
        return AuthenticateResult.Success(ticket);
    }

    protected override Task HandleChallengeAsync(AuthenticationProperties properties)
    {
        // Plain 401 — no WWW-Authenticate negotiation, no redirect (this is an API surface).
        Response.StatusCode = StatusCodes.Status401Unauthorized;
        return Task.CompletedTask;
    }

    private static string? ExtractKey(HttpRequest request)
    {
        if (request.Headers.TryGetValue(HeaderName, out var header))
        {
            var value = header.ToString();
            if (string.IsNullOrWhiteSpace(value) == false)
                return value.Trim();
        }

        var authorization = request.Headers.Authorization.ToString();
        const string bearer = "Bearer ";
        if (authorization.StartsWith(bearer, StringComparison.OrdinalIgnoreCase))
        {
            var token = authorization[bearer.Length..].Trim();
            if (string.IsNullOrWhiteSpace(token) == false)
                return token;
        }

        return null;
    }
}
