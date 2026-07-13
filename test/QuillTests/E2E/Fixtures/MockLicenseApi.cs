using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace QuillTests.E2E.Fixtures;

/// In-process stand-in for api.ravendb.net. Hosts the RavenDB-26783 endpoint
/// GET /api/v1/quill/licenses/{token} returning the embedded setup-package zip
/// bytes when {token} matches the configured key, 404 otherwise. Caller disposes;
/// the bound base URL is exposed for the appliance to dial.
public sealed class MockLicenseApi : IAsyncDisposable
{
    private readonly WebApplication _app;

    public string BaseAddress { get; }

    private MockLicenseApi(WebApplication app, string baseAddress)
    {
        _app = app;
        BaseAddress = baseAddress;
    }

    public static async Task<MockLicenseApi> StartAsync(string licenseKey, byte[] setupPackageZipBytes)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();

        app.MapGet("/api/v1/quill/licenses/{token}", (string token, HttpContext ctx) =>
        {
            if (!string.Equals(token, licenseKey, StringComparison.Ordinal))
                return Results.NotFound();

            return Results.File(setupPackageZipBytes,
                contentType: "application/zip",
                fileDownloadName: "setup-package.zip");
        });

        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var url = addresses?.Addresses.FirstOrDefault()
                  ?? throw new InvalidOperationException("MockLicenseApi failed to bind a port.");

        return new MockLicenseApi(app, url.TrimEnd('/'));
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
