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
        // (e.g. https://a.egor-ai.ravendb.run) and the matching admin client cert
        // sits at the package root. We connect via the public hostname (so the
        // wildcard server cert validates) — *.ravendb.run resolves to 127.0.0.1 via
        // public DNS, no /etc/hosts hack needed — but on the loopback HTTPS port
        // RavenDB now binds, since nginx owns :443 (see the port rewrite below).
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

        // A/settings.json being absent is the legit pre-activation pathway —
        // RavenDB hasn't been configured yet, the activation screen is live,
        // the unsecured loopback store in Create() carries us until the
        // operator redeems the license. Return false → fall through.
        var settingsFile = Path.Combine(options.SetupPackagePath, "A", "settings.json");
        if (!File.Exists(settingsFile))
            return false;

        // From here on, the setup package marker IS on disk. The appliance is
        // secure-mode-only — RavenDB has been restarted into secure mode and
        // is no longer listening on plain HTTP loopback, so any malformed-
        // package failure below must fail loudly. Returning false would
        // silently downgrade to the unsecured branch in Create() which
        // points at RavenDB's now-defunct :8080, leaving the appliance
        // permanently broken AND hiding the real cause.
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

        // Admin client cert sits at the package root (not under A/) so the
        // operator can pull it out for portal access without rummaging through
        // the per-node directory. Sorted-first cert pick keeps the choice
        // deterministic across filesystems if a future package ever ships
        // multiple admin PFXs (today there's always exactly one).
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

        // RavenDB's setup-wizard produces unprotected admin client PFXs by
        // default; passing `default` (empty span) matches the loader's
        // "no password" idiom. Wrap the load so a corrupt or password-
        // protected PFX surfaces a focused error instead of a raw
        // CryptographicException out of DI build.
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

        // nginx owns :443; RavenDB listens on the loopback internal port. Keep the wildcard-cert
        // hostname (resolves to loopback), swap the port. DisableTopologyUpdates so this single-node
        // store keeps the explicit URL and never adopts the advertised :443 PublicServerUrl (now nginx).
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
