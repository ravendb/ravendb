using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Raven.Quill.Auth;
using Raven.Quill.Contracts;
using Raven.Quill.Logging;
using Raven.Server.Logging;

namespace Raven.Quill.Endpoints;

public static class AuthEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/auth").WithTags("auth");

        group.MapPost("/login", LoginAsync)
            .WithName("auth.login")
            .Accepts<LoginRequest>("application/json")
            .Produces<AuthStatusResponse>()
            .Produces<AuthStatusResponse>(StatusCodes.Status401Unauthorized)
            .Produces<AuthStatusResponse>(StatusCodes.Status429TooManyRequests);

        group.MapPost("/logout", LogoutAsync)
            .WithName("auth.logout")
            .Produces(StatusCodes.Status204NoContent);

        group.MapGet("/status", GetStatusAsync)
            .WithName("auth.status")
            .Produces<AuthStatusResponse>();
    }

    private static async Task<IResult> LoginAsync(
        LoginRequest body, IApiKeyStore keys, LoginFailureLimiter limiter, QuillLogger<AuthLogger> logger, HttpContext ctx, CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.ApiKey) ||
            await keys.ValidateAsync(body.ApiKey, ct) == false)
        {
            if (logger.AuditEnabled)
                logger.Audit("LOGIN", "failed", ctx);
            var isLimited = limiter.RegisterFailure(ClientKey(ctx));
            return Results.Json(new AuthStatusResponse(false),
                statusCode: isLimited ? StatusCodes.Status429TooManyRequests : StatusCodes.Status401Unauthorized);
        }

        limiter.Reset(ClientKey(ctx));

        var identity = new ClaimsIdentity(CookieAuthenticationDefaults.AuthenticationScheme);
        identity.AddClaim(new Claim(ClaimTypes.Name, "operator"));
        var principal = new ClaimsPrincipal(identity);
        await ctx.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, principal);

        if (logger.AuditEnabled)
            logger.Audit("LOGIN", "succeeded", ctx, principal);

        return Results.Ok(new AuthStatusResponse(true));
    }

    // keep the CT param: avoids the RequestDelegate overload that discards IResult (ASP0016)
    private static async Task<IResult> LogoutAsync(QuillLogger<AuthLogger> logger, HttpContext ctx, CancellationToken ct)
    {
        var principal = ctx.User;
        await ctx.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        if (logger.AuditEnabled)
            logger.Audit("LOGOUT", "session ended", ctx, principal);
        return Results.NoContent();
    }

    private static string ClientKey(HttpContext ctx) =>
        ctx.Connection.RemoteIpAddress?.ToString() ?? ctx.Connection.Id ?? "unknown";

    private static async Task<IResult> GetStatusAsync(HttpContext ctx, CancellationToken ct)
    {
        var cookie = await ctx.AuthenticateAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        var apiKey = await ctx.AuthenticateAsync(ApiKeyAuthenticationHandler.SchemeName);
        return Results.Ok(new AuthStatusResponse(cookie.Succeeded || apiKey.Succeeded));
    }

    /// Log category for authentication audit lines, shared with the cookie events in Program.cs.
    internal sealed class AuthLogger;
}
