using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using Raven.Quill.Logging;
using Raven.Server.Logging;

namespace Raven.Quill.Auth;

public sealed class ApiKeyAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory loggerFactory,
    UrlEncoder encoder,
    IApiKeyStore keys,
    QuillLogger<ApiKeyAuthenticationHandler> logger) : AuthenticationHandler<AuthenticationSchemeOptions>(options, loggerFactory, encoder)
{
    public const string SchemeName = "ApiKey";
    public const string HeaderName = "X-Api-Key";

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var presented = ExtractKey(Request);
        if (string.IsNullOrEmpty(presented))
            return AuthenticateResult.NoResult();

        if (await keys.ValidateAsync(presented, Context.RequestAborted) == false)
        {
            if (logger.AuditEnabled)
                logger.Audit("AUTH", $"rejected (invalid API key) {Request.Method} {Uri.EscapeDataString(Request.Path)}", Context);
            return AuthenticateResult.Fail("invalid API key");
        }

        var identity = new ClaimsIdentity(SchemeName);
        identity.AddClaim(new Claim(ClaimTypes.Name, "operator"));
        var ticket = new AuthenticationTicket(new ClaimsPrincipal(identity), SchemeName);
        return AuthenticateResult.Success(ticket);
    }

    protected override Task HandleChallengeAsync(AuthenticationProperties properties)
    {
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
