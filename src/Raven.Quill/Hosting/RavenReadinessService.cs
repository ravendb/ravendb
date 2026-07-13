using Microsoft.Extensions.Options;
using Polly.Registry;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Infrastructure;

namespace Raven.Quill.Hosting;

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

            // The config DB holds the link-index/{token} routing pointers, which
            // carry @expires; enable Expiration so they self-clean once the link's
            // TTL elapses (idempotent on every startup). RavenDB-26775.
            await AppDatabaseFeatures.EnableExpirationAsync(store, opts.ConfigDatabase, stoppingToken);

            ready.MarkReady();

            // Flip bootstrap Ready only from the post-restart secure process — the one that STARTED with
            // the setup package on disk. On a first / unsecured start the package appears mid-process
            // (activation just extracted it and is about to restart), so File.Exists would be true against
            // the still-unsecured store; gating on the startup state keeps readiness from clobbering the
            // activation-owned Restarting -> Ready transition. Activation owns the flip on first run (s6
            // restart in containers, or MarkReady inline on unsupervised hosts).
            if (bootstrap.StartedWithSetupPackage)
            {
                logger.LogInformation("Process started with the setup package present; marking bootstrap Ready.");
                bootstrap.MarkReady();
            }
            else
            {
                logger.LogInformation(
                    "RavenDB reachable but the process started without a setup package at {Path}; " +
                    "bootstrap stays in its current phase until startup activation completes.",
                    opts.SetupPackagePath);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutting down before readiness — leave the flag false, exit quietly.
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RavenDB readiness probe gave up after {Timeout}.", opts.ReadinessOverallTimeout);
            ready.MarkFailed(ex.Message);

            if (File.Exists(GetSetupSettingsPath(opts)))
                bootstrap.MarkRestarting(ex.Message);
            else
                bootstrap.MarkFailed(ex.Message);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Host shutdown: ensure /healthz flips back to 503 the moment the
        // service is told to stop, even if ExecuteAsync is still mid-probe.
        // The bootstrap-state half is suppressed during an activation-triggered
        // restart — the endpoint has already set phase = Restarting and the
        // frontend is polling for that exact value. Downgrading to
        // NeedsActivation here would briefly mislead any in-flight status
        // probe before Kestrel actually stops accepting connections.
        if (bootstrap.Phase != BootstrapPhase.Restarting)
            bootstrap.MarkFailed("shutting down");

        ready.MarkFailed("shutting down");
        await base.StopAsync(cancellationToken);
    }

    private static string GetSetupSettingsPath(ApplianceOptions options) =>
        Path.Combine(options.SetupPackagePath, "A", "settings.json");
}
