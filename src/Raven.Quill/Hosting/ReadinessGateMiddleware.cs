using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Raven.Quill.Hosting;

public static class ReadinessGateMiddleware
{
    public static IApplicationBuilder UseReadinessGate(this WebApplication app) =>
        app.Use(async (context, next) =>
        {
            var path = context.Request.Path;
            // gate all /api/* except /bootstrap (SPA poll) and /auth (login)
            var gated = path.StartsWithSegments("/api") &&
                        !path.StartsWithSegments("/api/bootstrap") &&
                        !path.StartsWithSegments("/api/auth");

            if (gated)
            {
                var ready = context.RequestServices.GetRequiredService<IServerReady>();
                if (!ready.IsReady)
                {
                    context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                    context.Response.Headers.RetryAfter = "5";
                    // static body only: ready.LastError could leak internals to an unauth probe
                    await context.Response.WriteAsJsonAsync(new
                    {
                        error = "appliance is not ready yet; poll /api/bootstrap/status",
                    });
                    return;
                }
            }

            await next();
        });
}
