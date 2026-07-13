using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.Quill.Hosting;

namespace Raven.Quill.Infrastructure;

internal sealed class RavenHealthCheck(IBootstrapState bootstrap) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (bootstrap.Phase == BootstrapPhase.Ready)
            return Task.FromResult(HealthCheckResult.Healthy());

        // /healthz is plain text, so keep using the dedicated kebab-case mapper
        // instead of the JSON enum representation used by /api/bootstrap/status.
        var phase = bootstrap.Phase.ToWire();
        var description = bootstrap.Reason is { Length: > 0 } reason
            ? $"appliance not ready ({phase}): {reason}"
            : $"appliance not ready: {phase}";
        return Task.FromResult(HealthCheckResult.Unhealthy(description));
    }
}
