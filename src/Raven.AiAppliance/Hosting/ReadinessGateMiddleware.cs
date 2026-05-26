using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Raven.AiAppliance.Hosting;

/// <summary>
/// Short-circuits non-bootstrap /api/* requests with 503 until
/// <see cref="IServerReady"/> flips. Closes the design gap noted in
/// <c>WizardEndpoints.cs</c>: before this gate, wizard / apps / chat handlers
/// each issued a defensive <c>EnsureDatabaseAsync</c> on every hit so an
/// early request couldn't land before <see cref="RavenReadinessService"/> had
/// created the config database. The gate moves that guarantee from per-request
/// to per-process.
/// </summary>
/// <remarks>
/// Gating rule: every path under <c>/api/</c> except <c>/api/bootstrap/</c>.
/// <c>/api/bootstrap/*</c> is the only way out of <c>NeedsActivation</c>, so
/// it has to remain reachable while <c>IsReady</c> is false. Static assets
/// and <c>/healthz</c> live outside <c>/api/</c> and are never gated — the
/// SPA must render so it can poll <c>/api/bootstrap/status</c>, and liveness
/// probes need to work in every phase.
/// </remarks>
public static class ReadinessGateMiddleware
{
    public static IApplicationBuilder UseReadinessGate(this WebApplication app) =>
        app.Use(async (context, next) =>
        {
            var path = context.Request.Path;
            var gated = path.StartsWithSegments("/api") &&
                        !path.StartsWithSegments("/api/bootstrap");

            if (gated)
            {
                var ready = context.RequestServices.GetRequiredService<IServerReady>();
                if (!ready.IsReady)
                {
                    // C4 (Copilot review #4365219160): the response body is
                    // public-safe only. `ready.LastError` comes from
                    // IServerReady.MarkFailed call sites that pass ex.Message
                    // (license-API upstream errors, RavenDB probe failures,
                    // setup-package validation errors) and can include paths,
                    // hostnames, or stack-trace fragments. All MarkFailed
                    // sites already log the underlying error via ILogger; the
                    // gate just emits the static "not ready" string so an
                    // unauthenticated probe can't fish for internal detail.
                    context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                    context.Response.Headers.RetryAfter = "5";
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
