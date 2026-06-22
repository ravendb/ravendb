using System.Diagnostics;
using System.IO.Compression;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.AiHelper;

namespace Raven.AiAppliance.Hosting;

/// <summary>
/// First-run activation, driven at startup by the <c>QUILL_LICENSE_KEY</c> token (bound to
/// <see cref="ApplianceOptions.LicenseToken"/>) — replaces the old operator-triggered
/// <c>POST /api/bootstrap/redeem-license</c>. Retrieves the setup-package zip via
/// <see cref="ILicenseClient"/> (real license API in production, mounted zip in mock mode), unpacks
/// it into <see cref="ApplianceOptions.SetupPackagePath"/>, writes the admin-thumbprint marker, then
/// either signals s6 to restart RavenDB into secure mode and exits the .NET host (container) or flips
/// bootstrap to <see cref="BootstrapPhase.Ready"/> inline (unsupervised hosts / tests).
/// </summary>
/// <remarks>
/// Idempotent across restarts: <see cref="IBootstrapState.TryMarkRedeeming"/> only wins from
/// <see cref="BootstrapPhase.NeedsActivation"/>, so the post-restart start (where the setup package
/// is already on disk and the phase is <see cref="BootstrapPhase.Restarting"/>) is a no-op here.
/// </remarks>
public sealed class ApplianceActivationService(
    IBootstrapState bootstrap,
    IOptions<ApplianceOptions> options,
    ILicenseClient licenseClient,
    IHostApplicationLifetime lifetime,
    ILogger<ApplianceActivationService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;

        // Nothing to activate without a token or a mounted mock package — skip without touching the
        // bootstrap phase. Keeps unconfigured hosts (and tests that drive bootstrap manually) in their
        // current phase instead of forcing them through a doomed redemption / network call.
        var mockZipPresent = string.IsNullOrEmpty(opts.SetupPackageZipPath) == false && File.Exists(opts.SetupPackageZipPath);
        if (string.IsNullOrWhiteSpace(opts.LicenseToken) && mockZipPresent == false)
        {
            logger.LogInformation(
                "No QUILL_LICENSE_KEY and no mock setup package present; skipping startup activation " +
                "(appliance stays in NeedsActivation).");
            return;
        }

        // Only the fresh start (NeedsActivation) activates; a post-restart start already has the setup
        // package on disk (phase Restarting) and must not re-redeem.
        if (bootstrap.TryMarkRedeeming() == false)
        {
            logger.LogInformation(
                "Setup package already applied (bootstrap phase {Phase}); skipping startup activation.",
                bootstrap.Phase);
            return;
        }

        var tempZipPath = Path.Combine(Path.GetTempPath(), $"setup-package-{Guid.NewGuid():N}.zip");
        try
        {
            try
            {
                await using (var tempFile = File.Create(tempZipPath))
                    await licenseClient.DownloadSetupPackageToAsync(opts.LicenseToken ?? string.Empty, tempFile, stoppingToken);

                Directory.CreateDirectory(opts.SetupPackagePath);
                // Zip Slip protection: ZipFile.ExtractToDirectory (net9+) resolves each entry against
                // the target dir and throws if it escapes, so `../` / absolute entries are rejected.
                ZipFile.ExtractToDirectory(tempZipPath, opts.SetupPackagePath, overwriteFiles: true);
            }
            finally
            {
                try { File.Delete(tempZipPath); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    logger.LogDebug(ex, "Failed to delete temp zip {Path}", tempZipPath);
                }
            }

            logger.LogInformation("Setup package activated and unpacked to {Path}.", opts.SetupPackagePath);

            WriteAdminThumbprint(opts);

            if (string.IsNullOrEmpty(opts.RavenDbS6Service) == false)
            {
                RestartIntoSecureMode(opts);
                return;
            }

            // Unsupervised host (tests / local dotnet run): no s6 to restart RavenDB or bring us back.
            // Flip straight to Ready — the in-process store keeps talking to whatever RavenDB it has.
            bootstrap.MarkReady();
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            bootstrap.MarkFailed("activation cancelled");
        }
        catch (InvalidDataException ex)
        {
            // Corrupt / non-zip payload from the upstream — log detail, surface a generic reason
            // (the /api/bootstrap/status surface is public).
            logger.LogWarning(ex, "Activation: setup package was not a valid zip.");
            bootstrap.MarkFailed("activation failed: the setup package was invalid");
        }
        catch (LicenseRetrievalException ex)
        {
            logger.LogError(ex, "Activation: failed to retrieve the setup package.");
            bootstrap.MarkFailed("activation failed: could not retrieve the setup package");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Activation failed.");
            bootstrap.MarkFailed("activation failed; see server logs for details");
        }
    }

    /// Persist the admin client cert's thumbprint next to the unpacked package so the s6
    /// <c>01-ravendb</c> run script can export it as <c>RAVEN_Security_WellKnownCertificates_Admin</c>
    /// before RavenDB starts. Computed here (we already have the cert handle) rather than via openssl.
    private void WriteAdminThumbprint(ApplianceOptions opts)
    {
        var adminPfx = Directory
            .GetFiles(opts.SetupPackagePath, "admin.client.certificate.*.pfx")
            .OrderBy(p => p, StringComparer.Ordinal)
            .FirstOrDefault();
        if (adminPfx is null)
        {
            logger.LogWarning(
                "No admin client certificate (admin.client.certificate.*.pfx) found under {Path}; " +
                "skipping the admin-thumbprint marker — RavenDB will not trust the admin cert and the " +
                "secure store may fail to authenticate (appliance can hang in Restarting).",
                opts.SetupPackagePath);
            return;
        }

        // Empty password matches the setup-wizard PFXs (the s6 scripts load them with `-passin pass:`).
        using var cert = X509CertificateLoader.LoadPkcs12FromFile(adminPfx, password: "");
        File.WriteAllText(Path.Combine(opts.SetupPackagePath, "admin-thumbprint"), cert.Thumbprint);
    }

    private void RestartIntoSecureMode(ApplianceOptions opts)
    {
        // Container deployment: s6 supervises RavenDB + the web host. Kick RavenDB into secure mode
        // and exit ourselves; s6 brings us back, and the second start wires the secure IDocumentStore
        // against PublicServerUrl + the admin cert. Single-writer flow (guarded by TryMarkRedeeming);
        // the CAS bool is discarded intentionally.
        // Contract: a non-empty RavenDbS6Service means an s6-supervised host — that supervisor is what
        // brings both processes back after StopApplication below. The unsupervised path keys off an empty
        // RavenDbS6Service and never reaches here.
        bootstrap.TryMarkRestarting();

        try
        {
            using var s6 = Process.Start(new ProcessStartInfo("s6-svc", "-r " + opts.RavenDbS6Service)
            {
                UseShellExecute = false,
            });
            s6?.WaitForExit(5000);
        }
        catch (Exception ex)
        {
            // s6-svc missing while RavenDbS6Service is set is a misconfiguration: nothing restarts the
            // host after StopApplication below, so it strands. Log loud (Error, not Warning).
            logger.LogError(ex, "Could not signal s6 to restart RavenDB ({Service}); an s6 supervisor must restart the host.", opts.RavenDbS6Service);
        }

        logger.LogInformation("Activation complete; restarting .NET host to bind the secure IDocumentStore.");
        lifetime.StopApplication();
    }
}
