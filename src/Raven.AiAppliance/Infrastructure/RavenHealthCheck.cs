using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.AiAppliance.Hosting;

namespace Raven.AiAppliance.Infrastructure;

internal sealed class RavenHealthCheck(IBootstrapState bootstrap) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (bootstrap.Phase == BootstrapPhase.Ready)
            return Task.FromResult(HealthCheckResult.Healthy());

        // ToWire() is the kebab-case spelling shared with /api/bootstrap/status,
        // so /healthz descriptions and the bootstrap status endpoint stay in
        // sync (vs. ad-hoc `Phase.ToString().ToLowerInvariant()` which produced
        // "needsactivation" — no hyphen — and drifted from the status wire).
        var phase = bootstrap.Phase.ToWire();
        var description = bootstrap.Reason is { Length: > 0 } reason
            ? $"appliance not ready ({phase}): {reason}"
            : $"appliance not ready: {phase}";
        return Task.FromResult(HealthCheckResult.Unhealthy(description));
    }
}
