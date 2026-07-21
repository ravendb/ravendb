using Microsoft.Extensions.Options;
using Polly;
using Polly.Registry;
using Polly.Retry;
using Polly.Timeout;
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

    // Polly runs strategies in add-order (first = outermost): overall timeout wraps the retry, attempt timeout cuts each probe
    public static void ConfigureProbePipeline(ResiliencePipelineBuilder builder, ApplianceOptions opts)
    {
        builder
            .AddTimeout(new TimeoutStrategyOptions { Timeout = opts.ReadinessOverallTimeout })
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => ex is not OperationCanceledException),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = TimeSpan.FromMilliseconds(250),
                MaxDelay = TimeSpan.FromSeconds(2),
                MaxRetryAttempts = int.MaxValue,
            })
            .AddTimeout(new TimeoutStrategyOptions { Timeout = opts.ReadinessAttemptTimeout });
    }

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

            while (stoppingToken.IsCancellationRequested == false)
            {
                try
                {
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

                    return;
                }
                catch (Exception ex) when (stoppingToken.IsCancellationRequested == false)
                {
                    logger.LogError(ex,
                        "RavenDB readiness probe failed after {Timeout}; retrying in {Delay}.",
                        opts.ReadinessOverallTimeout, opts.ReadinessInitialDelay);
                    ready.MarkFailed(ex.Message);

                    if (File.Exists(GetSetupSettingsPath(opts)))
                        bootstrap.MarkRestarting("ravendb is not reachable: " + ex.Message);
                    else
                        bootstrap.MarkFailed("ravendb is not reachable: " + ex.Message);

                    await Task.Delay(opts.ReadinessInitialDelay, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
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
