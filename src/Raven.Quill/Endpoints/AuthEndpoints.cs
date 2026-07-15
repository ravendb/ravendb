using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Raven.Quill.Auth;
using Raven.Quill.Contracts;

namespace Raven.Quill.Endpoints;

public static class AuthEndpoints
{
    public const string LoginRateLimitPolicy = "auth-login";

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/auth").WithTags("auth");

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

    // keep the CT param: avoids the RequestDelegate overload that discards IResult (ASP0016)
    private static async Task<IResult> LogoutAsync(HttpContext ctx, CancellationToken ct)
    {
        await ctx.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        return Results.NoContent();
    }

    private static async Task<IResult> GetStatusAsync(HttpContext ctx, CancellationToken ct)
    {
        var cookie = await ctx.AuthenticateAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        var apiKey = await ctx.AuthenticateAsync(ApiKeyAuthenticationHandler.SchemeName);
        return Results.Ok(new AuthStatusResponse(cookie.Succeeded || apiKey.Succeeded));
    }
}
