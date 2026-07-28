using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Raven.Client.Documents;
using Raven.Quill.Auth;
using Raven.Quill.Hosting;

namespace QuillTests.E2E.Fixtures;

/// Hosts the appliance Program for tests: seeds a known operator API key, swaps in the test's in-process
/// IDocumentStore, and flips IServerReady ready. Every CreateClient() carries the TestApiKey by default;
/// unauthenticated-path tests remove it.
public sealed class ApplianceWebApplicationFactory : WebApplicationFactory<Program>
{
    public const string TestApiKey = "test-api-key";

    private readonly string _setupPackagePath;
    private readonly IDocumentStore _applianceStore;
    private readonly Action<ApplianceOptions>? _configureOptions;
    private readonly Action<IServiceCollection>? _configureServices;

    public ApplianceWebApplicationFactory(
        string setupPackagePath,
        IDocumentStore applianceStore,
        Action<ApplianceOptions>? configureOptions = null,
        Action<IServiceCollection>? configureServices = null)
    {
        _setupPackagePath = setupPackagePath;
        _applianceStore = applianceStore;
        _configureOptions = configureOptions;
        _configureServices = configureServices;
    }

    protected override IHost CreateHost(IHostBuilder builder)
    {
        builder.ConfigureServices(services =>
        {
            services.PostConfigure<ApplianceOptions>(opts =>
            {
                opts.SetupPackagePath = _setupPackagePath;
                opts.ApiKey = TestApiKey;
                _configureOptions?.Invoke(opts);
            });

            services.RemoveAll<IDocumentStore>();
            services.AddSingleton(_applianceStore);

            var toRemove = services
                .Where(d => d.ImplementationType == typeof(RavenReadinessService))
                .ToList();
            foreach (var d in toRemove)
                services.Remove(d);

            // last, so a test's swap wins over everything the appliance registered
            _configureServices?.Invoke(services);
        });

        var host = base.CreateHost(builder);
        host.Services.GetRequiredService<IServerReady>().MarkReady();
        return host;
    }

    protected override void ConfigureClient(HttpClient client)
    {
        base.ConfigureClient(client);
        client.DefaultRequestHeaders.Add(ApiKeyAuthenticationHandler.HeaderName, TestApiKey);
    }
}
