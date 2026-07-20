using FastTests;
using Polly;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for the readiness probe's resilience pipeline
/// (<see cref="RavenReadinessService.ConfigureProbePipeline"/>) and the
/// <see cref="IServerReady"/>/<see cref="IBootstrapState"/> transitions the
/// re-probe loop depends on. Polly executes strategies in add-order (first added =
/// outermost), so the overall timeout must be added before the retry, and the
/// per-attempt timeout after it.
/// </summary>
public class ReadinessPipelineTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Probe_pipeline_cuts_a_hung_attempt_and_retries()
    {
        var opts = new ApplianceOptions
        {
            ReadinessAttemptTimeout = TimeSpan.FromMilliseconds(200),
            ReadinessOverallTimeout = TimeSpan.FromSeconds(30),
        };
        var builder = new ResiliencePipelineBuilder();
        RavenReadinessService.ConfigureProbePipeline(builder, opts);
        var pipeline = builder.Build();

        var attempts = 0;
        await pipeline.ExecuteAsync(async ct =>
        {
            attempts++;
            if (attempts == 1)
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        }, CancellationToken.None);

        Assert.Equal(2, attempts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ServerReadyFlag_recovers_from_failed_to_ready()
    {
        var flag = new ServerReadyFlag();

        flag.MarkFailed("raven not reachable");
        Assert.False(flag.IsReady);
        Assert.Equal("raven not reachable", flag.LastError);

        flag.MarkReady();
        Assert.True(flag.IsReady);
        Assert.Null(flag.LastError);
    }
}
