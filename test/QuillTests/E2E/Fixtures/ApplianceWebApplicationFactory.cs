using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Raven.Client.Documents;
using Raven.Quill.Auth;
using Raven.Quill.Hosting;

namespace QuillTests.E2E.Fixtures;

/// Hosts the appliance Program for tests:
///   - Points ApplianceOptions at the test's mock license API and a temp setup-package dir, and seeds
///     a known operator API key (<see cref="TestApiKey"/>) so the now-gated admin endpoints authenticate.
///   - Replaces IDocumentStore with the test's in-process store so wizard endpoints exercise real
///     RavenDB code paths without launching a second instance.
///   - Removes RavenReadinessService and flips IServerReady ready (the test store is already ready).
///   - Every CreateClient() carries the TestApiKey header by default; tests that exercise the
///     unauthenticated path remove it. Startup activation is inert unless a license token / mock zip is
///     configured (see ApplianceActivationService), so it stays out of the way of non-activation tests.
internal sealed class ApplianceWebApplicationFactory : WebApplicationFactory<Program>
{
    /// <summary>Operator API key seeded into the appliance and sent by default on every test client.</summary>
    public const string TestApiKey = "test-api-key";

    private readonly string _licenseApiUrl;
    private readonly string _setupPackagePath;
    private readonly IDocumentStore _applianceStore;
    private readonly Action<ApplianceOptions>? _configureOptions;

    public ApplianceWebApplicationFactory(
        string licenseApiUrl,
        string setupPackagePath,
        IDocumentStore applianceStore,
        Action<ApplianceOptions>? configureOptions = null)
    {
        _licenseApiUrl = licenseApiUrl;
        _setupPackagePath = setupPackagePath;
        _applianceStore = applianceStore;
        _configureOptions = configureOptions;
    }

    protected override IHost CreateHost(IHostBuilder builder)
    {
        builder.ConfigureServices(services =>
        {
            services.PostConfigure<ApplianceOptions>(opts =>
            {
                opts.LicenseApiUrl = _licenseApiUrl;
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
