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
            // grace period: RavenDB needs ~10-15s; earlier probes just spam errors
            if (opts.ReadinessInitialDelay > TimeSpan.Zero)
            {
                logger.LogInformation(
                    "Waiting {Delay} for RavenDB to start before probing readiness...",
                    opts.ReadinessInitialDelay);
                await Task.Delay(opts.ReadinessInitialDelay, stoppingToken);
            }

            await pipeline.ExecuteAsync(async ct =>
            {
                await store.Maintenance.Server.SendAsync(new GetBuildNumberOperation(), ct);
            }, stoppingToken);

            var created = await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, stoppingToken);
            logger.LogInformation(
                "RavenDB ready at {Url}; config database {Database} {Action}.",
                opts.RavenUrl, opts.ConfigDatabase, created ? "created" : "already present");

            ready.MarkReady();

            // flip bootstrap Ready only from the post-restart secure start (don't clobber activation)
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
        if (bootstrap.Phase != BootstrapPhase.Restarting)
            bootstrap.MarkFailed("shutting down");

        ready.MarkFailed("shutting down");
        await base.StopAsync(cancellationToken);
    }

    private static string GetSetupSettingsPath(ApplianceOptions options) =>
        Path.Combine(options.SetupPackagePath, "A", "settings.json");
}
