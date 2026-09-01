using Microsoft.Extensions.Options;
using Polly;
using Polly.Registry;
using Polly.Retry;
using Polly.Timeout;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Infrastructure;

using Raven.Quill.Logging;

namespace Raven.Quill.Hosting;

public sealed class RavenReadinessService(
    IDocumentStore store,
    IOptions<ApplianceOptions> options,
    IServerReady ready,
    IBootstrapState bootstrap,
    ResiliencePipelineProvider<string> pipelines,
    QuillLogger<RavenReadinessService> logger) : BackgroundService
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
            if (string.IsNullOrEmpty(opts.RavenDbS6Service) == false && bootstrap.StartedWithSetupPackage == false)
            {
                if (logger.IsInfoEnabled)
                    logger.Info("Started without the setup package; waiting for activation to restart the host before probing RavenDB.");
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }

            // grace period: RavenDB needs ~10-15s; earlier probes just spam errors
            if (opts.ReadinessInitialDelay > TimeSpan.Zero)
            {
                if (logger.IsInfoEnabled)
                    logger.Info(
                        $"Waiting {opts.ReadinessInitialDelay} for RavenDB to start before probing " +
                        "readiness...");
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

                    var r = await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, DatabaseLockMode.PreventDeletesError, stoppingToken);
                    if (logger.IsInfoEnabled)
                        logger.Info(
                            $"RavenDB ready at {opts.RavenUrl}; config database {opts.ConfigDatabase} " +
                            $"{(r.Created ? "created" : "already present")}.");

                    ready.MarkReady();

                    // flip bootstrap Ready only from the post-restart secure start (don't clobber activation)
                    if (bootstrap.StartedWithSetupPackage)
                    {
                        if (logger.IsInfoEnabled)
                            logger.Info("Process started with the setup package present; marking bootstrap Ready.");
                        bootstrap.MarkReady();
                    }
                    else
                    {
                        if (logger.IsInfoEnabled)
                            logger.Info(
                                "RavenDB reachable but the process started without a setup package at " +
                                $"{opts.SetupPackagePath}; bootstrap stays in its current phase until " +
                                "startup activation completes.");
                    }

                    return;
                }
                catch (Exception ex) when (stoppingToken.IsCancellationRequested == false)
                {
                    if (logger.IsErrorEnabled)
                        logger.Error(ex,
                            $"RavenDB readiness probe failed after {opts.ReadinessOverallTimeout}; " +
                            $"retrying in {opts.ReadinessInitialDelay}.");
                    ready.MarkFailed(ex.Message);

                    if (File.Exists(opts.SetupNodeSettingsPath))
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

}
