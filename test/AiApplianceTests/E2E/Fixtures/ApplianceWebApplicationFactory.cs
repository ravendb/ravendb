using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Raven.AiAppliance.Hosting;
using Raven.Client.Documents;

namespace AiApplianceTests.E2E.Fixtures;

/// Hosts the appliance Program for the E2E test:
///   - Points ApplianceOptions at the test's mock license API and a temp setup-package dir.
///   - Replaces IDocumentStore with the test's in-process store so wizard endpoints
///     exercise real RavenDB code paths without launching a second instance.
///   - Removes RavenReadinessService — the test store is already ready; we don't want
///     the probe loop firing against the (unused) default RavenDB URL.
internal sealed class ApplianceWebApplicationFactory : WebApplicationFactory<Program>
{
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

        return base.CreateHost(builder);
    }
}
