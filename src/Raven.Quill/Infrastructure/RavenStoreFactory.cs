using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Hosting;

namespace Raven.Quill.Infrastructure;

public static class RavenStoreFactory
{
    public static IDocumentStore Create(ApplianceOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.RavenUrl, nameof(ApplianceOptions.RavenUrl));
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ConfigDatabase, nameof(ApplianceOptions.ConfigDatabase));

        if (TryCreateSecureStore(options, out var secureStore))
            return secureStore;

        var store = new DocumentStore
        {
            Urls = [options.RavenUrl],
            Database = options.ConfigDatabase,
        };
        store.Initialize();
        return store;
    }

    public static IDocumentStore Create(IOptions<ApplianceOptions> options) =>
        Create(options.Value);

    public static async Task<bool> EnsureDatabaseAsync(IDocumentStore store, string database, CancellationToken ct = default)
    {
        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database), ct);
        if (record is not null)
            return false;

        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database)), ct);
        return true;
    }

    private static bool TryCreateSecureStore(ApplianceOptions options, out IDocumentStore store)
    {
        store = null!;

        var settingsFile = Path.Combine(options.SetupPackagePath, "A", "settings.json");
        if (!File.Exists(settingsFile))
            return false;

        string? publicUrl;
        using (var stream = File.OpenRead(settingsFile))
        using (var doc = JsonDocument.Parse(stream))
        {
            if (!doc.RootElement.TryGetProperty("PublicServerUrl", out var pub) ||
                pub.ValueKind != JsonValueKind.String)
            {
                throw new InvalidOperationException(
                    $"Setup package at '{settingsFile}' is malformed: PublicServerUrl is missing or not a string. " +
                    "Re-run activation with a valid setup package.");
            }

            publicUrl = pub.GetString();
        }

        if (string.IsNullOrWhiteSpace(publicUrl))
        {
            throw new InvalidOperationException(
                $"Setup package at '{settingsFile}' has an empty PublicServerUrl. " +
                "Re-run activation with a valid setup package.");
        }

        var adminPfx = Directory.Exists(options.SetupPackagePath)
            ? Directory
                .GetFiles(options.SetupPackagePath, "admin.client.certificate.*.pfx")
                .OrderBy(p => p, StringComparer.Ordinal)
                .FirstOrDefault()
            : null;
        if (adminPfx is null)
        {
            throw new InvalidOperationException(
                $"Setup package at '{options.SetupPackagePath}' has no admin.client.certificate.*.pfx file. " +
                "Re-run activation with a valid setup package.");
        }

        X509Certificate2 adminCert;
        try
        {
            adminCert = X509CertificateLoader.LoadPkcs12FromFile(adminPfx, password: default);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to load admin client certificate from '{adminPfx}'. " +
                "The PFX may be corrupt or password-protected. " +
                "Re-run activation with a valid setup package.",
                ex);
        }

        var connectUrl = new UriBuilder(publicUrl) { Port = options.RavenInternalPort }.Uri.ToString().TrimEnd('/');

        var secured = new DocumentStore
        {
            Urls = [connectUrl],
            Database = options.ConfigDatabase,
            Certificate = adminCert,
            Conventions = { DisableTopologyUpdates = true },
        };
        secured.Initialize();
        store = secured;
        return true;
    }
}
