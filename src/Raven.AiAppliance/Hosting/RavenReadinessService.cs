using Microsoft.Extensions.Options;
using Polly.Registry;
using Raven.AiAppliance.Infrastructure;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;

namespace Raven.AiAppliance.Hosting;

public sealed class RavenReadinessService(
    IDocumentStore store,
    IOptions<ApplianceOptions> options,
    IServerReady ready,
    IBootstrapState bootstrap,
    ResiliencePipelineProvider<string> pipelines,
    ILogger<RavenReadinessService> logger) : BackgroundService
{
    public const string PipelineName = "raven-startup";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;
        var pipeline = pipelines.GetPipeline(PipelineName);

        try
        {
            // Silent grace period. RavenDB takes ~10-15s to start; pinging it
            // earlier just spams the console with connection-refused errors.
            if (opts.ReadinessInitialDelay > TimeSpan.Zero)
            {
                logger.LogInformation(
                    "Waiting {Delay} for RavenDB to start before probing readiness...",
                    opts.ReadinessInitialDelay);
                await Task.Delay(opts.ReadinessInitialDelay, stoppingToken);
            }

            await pipeline.ExecuteAsync(async ct =>
            {
                // Per-attempt timeout is enforced by the pipeline's inner
                // AddTimeout strategy (see Program.cs). It raises
                // TimeoutRejectedException — which the surrounding retry handles
                // — so a single slow probe no longer aborts the whole flow.
                await store.Maintenance.Server.SendAsync(new GetBuildNumberOperation(), ct);
            }, stoppingToken);

            var created = await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, stoppingToken);
            logger.LogInformation(
                "RavenDB ready at {Url}; config database {Database} {Action}.",
                opts.RavenUrl, opts.ConfigDatabase, created ? "created" : "already present");

            ready.MarkReady();
            bootstrap.MarkReady();
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutting down before readiness — leave the flag false, exit quietly.
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RavenDB readiness probe gave up after {Timeout}.", opts.ReadinessOverallTimeout);
            ready.MarkFailed(ex.Message);
            bootstrap.MarkFailed(ex.Message);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Host shutdown: ensure /healthz flips back to 503 the moment the
        // service is told to stop, even if ExecuteAsync is still mid-probe.
        // Idempotent — if we're already past MarkReady() the next observer
        // sees `shutting down` instead of stale `ready`. (MarkFailed reverts
        // the phase to NeedsActivation; that's fine on the way out.)
        bootstrap.MarkFailed("shutting down");
        ready.MarkFailed("shutting down");
        await base.StopAsync(cancellationToken);
    }
}
