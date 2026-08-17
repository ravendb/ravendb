using System.Net;
using FastTests;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Raven.Client.Documents;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class HealthEndpointsTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill | RavenTestCategory.Monitoring)]
    public async Task Returns_503_before_bootstrap_phase_is_ready()
    {
        using var factory = new Factory(GetDocumentStore());
        factory.Bootstrap.MarkFailed("not yet");

        var response = await factory.CreateClient().GetAsync("/healthz");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill | RavenTestCategory.Monitoring)]
    public async Task Returns_200_once_bootstrap_phase_is_ready()
    {
        using var factory = new Factory(GetDocumentStore());
        factory.Bootstrap.MarkReady();

        var response = await factory.CreateClient().GetAsync("/healthz");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    public sealed class Factory(IDocumentStore store) : WebApplicationFactory<Program>
    {
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
                services.AddSingleton(Bootstrap);

                services.RemoveAll<IDocumentStore>();
                services.AddSingleton(store);

                // Drop only RavenReadinessService — RemoveAll<IHostedService>() would also kill GenericWebHostService.
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
