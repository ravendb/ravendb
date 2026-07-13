using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Raven.Quill.Contracts;
using Raven.Quill.Hosting;

namespace Raven.Quill.Endpoints;

/// <summary>
/// First-run status endpoint. Activation itself is startup-driven by
/// <see cref="ApplianceActivationService"/> (the <c>QUILL_LICENSE_KEY</c> token) — there is no
/// operator-triggered redeem call anymore. The frontend polls <c>/api/bootstrap/status</c> to
/// boot-gate the SPA until the phase reaches <see cref="BootstrapPhase.Ready"/>. It stays reachable
/// in every phase (the only window into NeedsActivation / Redeeming / Restarting), so the readiness
/// gate exempts <c>/api/bootstrap</c> and it carries no authentication.
/// </summary>
public static class BootstrapEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/bootstrap");
        group.MapGet("/status", GetStatus)
            .WithName("bootstrap.status")
            .Produces<BootstrapStatusResponse>();
    }

    private static IResult GetStatus(IBootstrapState state) =>
        Results.Ok(new BootstrapStatusResponse(state.Phase, state.Reason));
}
