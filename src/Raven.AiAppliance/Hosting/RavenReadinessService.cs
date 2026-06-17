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

            // The config DB holds the link-index/{token} routing pointers, which
            // carry @expires; enable Expiration so they self-clean once the link's
            // TTL elapses (idempotent on every startup). RavenDB-26775.
            await AppDatabaseFeatures.EnableExpirationAsync(store, opts.ConfigDatabase, stoppingToken);

            ready.MarkReady();

            // Bootstrap only flips to Ready once activation has produced a setup
            // package on disk — pre-activation we still want RavenDB up so the
            // appliance has somewhere to persist state, but the wizard / chat
            // endpoints have to stay 503 until the secure config is applied.
            var setupSettings = GetSetupSettingsPath(opts);
            if (File.Exists(setupSettings))
            {
                logger.LogInformation("Setup package present at {Path}; marking bootstrap Ready.", opts.SetupPackagePath);
                bootstrap.MarkReady();
            }
            else
            {
                logger.LogInformation(
                    "RavenDB reachable but setup package not yet extracted at {Path}; bootstrap stays NeedsActivation until startup activation completes.",
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
