using System.Net;
using FastTests;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Raven.AiAppliance.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class HealthEndpointsTests(ITestOutputHelper output, HealthEndpointsTests.Factory factory)
    : RavenTestBase(output), IClassFixture<HealthEndpointsTests.Factory>
{
    private readonly Factory _factory = factory;

    [RavenFact(RavenTestCategory.Monitoring)]
    public async Task Returns_503_before_bootstrap_phase_is_ready()
    {
        _factory.Bootstrap.MarkFailed("not yet");
        var client = _factory.CreateClient();
        var response = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public async Task Returns_200_once_bootstrap_phase_is_ready()
    {
        _factory.Bootstrap.MarkReady();
        var client = _factory.CreateClient();
        var response = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    public sealed class Factory : WebApplicationFactory<Program>
    {
        // RavenHealthCheck reads IBootstrapState (not IServerReady) — controlling
        // that flag is what flips /healthz between 503 and 200.
        public IBootstrapState Bootstrap { get; } = new BootstrapStateFlag(
            Microsoft.Extensions.Options.Options.Create(new ApplianceOptions
            {
                SetupPackagePath = Path.Combine(
                    Path.GetTempPath(),
                    nameof(HealthEndpointsTests),
                    Guid.NewGuid().ToString("N"))
            }));

        protected override IHost CreateHost(IHostBuilder builder)
        {
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<IBootstrapState>();
                services.AddSingleton<IBootstrapState>(Bootstrap);

                // Drop only RavenReadinessService — RemoveAll<IHostedService>()
                // would also kill GenericWebHostService and leave the test host
                // unable to serve traffic.
                var toRemove = services
                    .Where(d => d.ImplementationType == typeof(RavenReadinessService))
                    .ToList();
                foreach (var d in toRemove)
                    services.Remove(d);
            });
            return base.CreateHost(builder);
        }
    }
}
