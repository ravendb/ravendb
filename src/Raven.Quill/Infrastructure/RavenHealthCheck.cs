using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Hosting;

namespace Raven.Quill.Infrastructure;

internal sealed class RavenHealthCheck(IBootstrapState bootstrap, IDocumentStore store) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (bootstrap.Phase != BootstrapPhase.Ready)
        {
            var phase = bootstrap.Phase.ToWire();
            var description = bootstrap.Reason is { Length: > 0 } reason
                ? $"appliance not ready ({phase}): {reason}"
                : $"appliance not ready: {phase}";

            return HealthCheckResult.Unhealthy(description);
        }

        try
        {
            await store.Maintenance.Server.SendAsync(new GetBuildNumberOperation(), cancellationToken);

            return HealthCheckResult.Healthy();
        }
        catch (Exception e)
        {
            return HealthCheckResult.Unhealthy($"ravendb is not reachable: {e.Message}");
        }
    }
}
