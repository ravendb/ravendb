using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace Raven.AiAppliance.Infrastructure;

public static class RavenStoreFactory
{
    public static IDocumentStore Create(ApplianceOptions options)
    {
        // Specific paramName per field so the stack trace pinpoints the bad
        // setting (vs. "options" which would just say "the whole options bag
        // is wrong"). Belt-and-braces alongside the [Required] data-annotation
        // checks that run on IOptions binding.
        ArgumentException.ThrowIfNullOrWhiteSpace(options.RavenUrl, nameof(ApplianceOptions.RavenUrl));
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ConfigDatabase, nameof(ApplianceOptions.ConfigDatabase));

        // Post-activation: A/settings.json carries the LE-bound PublicServerUrl
        // (e.g. https://a.egor-ai.ravendb.run) and the matching admin client
        // cert sits at the package root. Connect via the public hostname so
        // RavenDB's server cert validates — /etc/hosts (written by the s6 run
        // script) pins that hostname back to 127.0.0.1 inside the container.
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
            if (!doc.RootElement.TryGetProperty("PublicServerUrl", out var pub))
                return false;
            publicUrl = pub.GetString();
        }

        if (string.IsNullOrWhiteSpace(publicUrl))
            return false;

        // Admin client cert sits at the package root (not under A/) so the
        // operator can pull it out for portal access without rummaging through
        // the per-node directory.
        var certs = Directory.Exists(options.SetupPackagePath)
            ? Directory.GetFiles(options.SetupPackagePath, "admin.client.certificate.*.pfx")
            : [];
        if (certs.Length == 0)
            return false;

        // RavenDB's setup-wizard produces unprotected admin client PFXs by
        // default; passing `default` (empty span) matches the loader's
        // "no password" idiom.
        var adminCert = X509CertificateLoader.LoadPkcs12FromFile(certs[0], password: default);

        var secured = new DocumentStore
        {
            Urls = [publicUrl],
            Database = options.ConfigDatabase,
            Certificate = adminCert,
        };
        secured.Initialize();
        store = secured;
        return true;
    }
}
