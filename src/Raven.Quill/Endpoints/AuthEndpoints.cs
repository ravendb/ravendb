using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Raven.Quill.Auth;
using Raven.Quill.Contracts;

namespace Raven.Quill.Endpoints;

/// <summary>
/// Operator authentication for the dashboard. <c>POST /api/auth/login</c> validates the API key and
/// issues a session cookie (the <c>dashboard.*</c> credential); <c>api.*</c> clients skip this and
/// pass the key per request via header. These endpoints are anonymous and exempt from the readiness
/// gate so the SPA can authenticate as soon as it boots.
/// </summary>
public static class AuthEndpoints
{
    /// <summary>Per-IP rate-limit policy that blunts API-key brute-forcing on the login endpoint.</summary>
    public const string LoginRateLimitPolicy = "auth-login";

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/auth").WithTags("auth");

        // .RequireRateLimiting returns the base IEndpointConventionBuilder, so the RouteHandlerBuilder
        // metadata calls (.Accepts/.Produces) must come before it.
        group.MapPost("/login", LoginAsync)
            .WithName("auth.login")
            .Accepts<LoginRequest>("application/json")
            .Produces<AuthStatusResponse>()
            .Produces<AuthStatusResponse>(StatusCodes.Status401Unauthorized)
            .RequireRateLimiting(LoginRateLimitPolicy);

        group.MapPost("/logout", LogoutAsync)
            .WithName("auth.logout")
            .Produces(StatusCodes.Status204NoContent);

        group.MapGet("/status", GetStatusAsync)
            .WithName("auth.status")
            .Produces<AuthStatusResponse>();
    }

    private static async Task<IResult> LoginAsync(LoginRequest body, IApiKeyStore keys, HttpContext ctx, CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.ApiKey) ||
            await keys.ValidateAsync(body.ApiKey, ct) == false)
        {
            return Results.Json(new AuthStatusResponse(false), statusCode: StatusCodes.Status401Unauthorized);
        }

        var identity = new ClaimsIdentity(CookieAuthenticationDefaults.AuthenticationScheme);
        identity.AddClaim(new Claim(ClaimTypes.Name, "operator"));
        await ctx.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, new ClaimsPrincipal(identity));

        return Results.Ok(new AuthStatusResponse(true));
    }

    // The CancellationToken parameter is also what keeps these handlers off the RequestDelegate
    // overload (a single HttpContext param would discard the returned IResult — ASP0016).
    private static async Task<IResult> LogoutAsync(HttpContext ctx, CancellationToken ct)
    {
        await ctx.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        return Results.NoContent();
    }

    private static async Task<IResult> GetStatusAsync(HttpContext ctx, CancellationToken ct)
    {
        // Reflect either credential: a valid session cookie or a valid API-key header. UseAuthentication
        // only auto-runs the default scheme, so authenticate both explicitly here.
        var cookie = await ctx.AuthenticateAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        var apiKey = await ctx.AuthenticateAsync(ApiKeyAuthenticationHandler.SchemeName);
        return Results.Ok(new AuthStatusResponse(cookie.Succeeded || apiKey.Succeeded));
    }
}
