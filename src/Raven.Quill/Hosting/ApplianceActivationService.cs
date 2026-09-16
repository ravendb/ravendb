using System.IO.Compression;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Options;
using Raven.Quill.AiHelper;
using Raven.Quill.Logging;
using Raven.Server.Logging;

namespace Raven.Quill.Hosting;

public sealed class ApplianceActivationService(
    IBootstrapState bootstrap,
    IOptions<ApplianceOptions> options,
    ILicenseClient licenseClient,
    IHostApplicationLifetime lifetime,
    QuillLogger<ApplianceActivationService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;

        if (string.IsNullOrWhiteSpace(opts.LicenseKey))
        {
            if (logger.IsInfoEnabled)
                logger.Info(
                    "No QUILL_LICENSE_KEY; skipping startup activation (appliance stays in NeedsActivation).");
            return;
        }

        if (bootstrap.TryMarkRedeeming() == false)
        {
            if (logger.IsInfoEnabled)
                logger.Info(
                    $"Setup package already applied (bootstrap phase {bootstrap.Phase}); " +
                    "skipping startup activation.");
            return;
        }

        var tempZipPath = Path.Combine(Path.GetTempPath(), $"setup-package-{Guid.NewGuid():N}.zip");
        try
        {
            try
            {
                await using (var tempFile = File.Create(tempZipPath))
                    await DownloadWithRetryAsync(opts.LicenseKey ?? string.Empty, tempFile, stoppingToken);

                var target = Path.TrimEndingDirectorySeparator(opts.SetupPackagePath);
                var staging = target + ".incoming";
                var previous = target + ".old";

                if (Directory.Exists(staging))
                    Directory.Delete(staging, recursive: true);

                Directory.CreateDirectory(staging);
                // zip-slip guard: ExtractToDirectory rejects ../ and absolute entries
                ZipFile.ExtractToDirectory(tempZipPath, staging, overwriteFiles: true);

                WriteAdminThumbprint(staging);

                // Promote staging by renaming the current package aside first, so a complete package is on
                // disk at every instant: a crash between the renames leaves either the old or the new one
                // intact, never nothing. Renaming (not deleting then moving) also frees the target name
                // synchronously, avoiding the Windows NTFS delete-pending race that can make the Move throw.
                if (Directory.Exists(previous))
                    Directory.Delete(previous, recursive: true);
                if (Directory.Exists(target))
                    Directory.Move(target, previous);

                Directory.Move(staging, target);

                if (Directory.Exists(previous))
                    Directory.Delete(previous, recursive: true);
            }
            finally
            {
                try { File.Delete(tempZipPath); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    if (logger.IsDebugEnabled)
                        logger.Debug(ex, $"Failed to delete temp zip {tempZipPath}");
                }
            }

            if (logger.IsInfoEnabled)
                logger.Info($"Setup package activated and unpacked to {opts.SetupPackagePath}.");
            if (logger.AuditEnabled)
                logger.Audit("ACTIVATION", $"setup package unpacked to '{opts.SetupPackagePath}'", context: null);

            if (string.IsNullOrEmpty(opts.RavenDbS6Service) == false)
            {
                RestartIntoSecureMode();
                return;
            }

            bootstrap.MarkReady();
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            bootstrap.MarkFailed("activation cancelled");
        }
        catch (InvalidDataException ex)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(ex, "Activation: setup package was not a valid zip.");
            bootstrap.MarkFailed("activation failed: the setup package was invalid");
        }
        catch (LicenseKeyNotFoundException ex)
        {
            if (logger.IsErrorEnabled)
                logger.Error(ex, "Activation: the license key has no setup package.");
            bootstrap.MarkFailed("activation failed: the license key was not recognized");
        }
        catch (Exception ex)
        {
            if (logger.IsErrorEnabled)
                logger.Error(ex, "Activation failed.");
            if (logger.AuditEnabled)
                logger.Audit("ACTIVATION", "failed", context: null);
            bootstrap.MarkFailed("activation failed; see server logs for details");
        }
    }

    private static readonly TimeSpan DownloadRetryDelay = TimeSpan.FromSeconds(30);

    private async Task DownloadWithRetryAsync(string licenseKey, Stream destination, CancellationToken ct)
    {
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await licenseClient.DownloadSetupPackageToAsync(licenseKey, destination, ct);
                return;
            }
            catch (LicenseRetrievalException ex)
            {
                attempt++;
                if (logger.IsInfoEnabled)
                    logger.Info(
                        $"Setup package not available yet (attempt {attempt}: {ex.Message}); " +
                        $"retrying in {DownloadRetryDelay.TotalSeconds}s.");

                // A partial write from the failed attempt must not corrupt the next one.
                destination.SetLength(0);
                destination.Position = 0;

                await Task.Delay(DownloadRetryDelay, ct);
            }
        }
    }

    private void WriteAdminThumbprint(string packagePath)
    {
        var adminPfx = Directory
            .GetFiles(packagePath, "admin.client.certificate.*.pfx")
            .OrderBy(p => p, StringComparer.Ordinal)
            .FirstOrDefault();
        if (adminPfx is null)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(
                    $"No admin client certificate (admin.client.certificate.*.pfx) found under " +
                    $"{packagePath}; skipping the admin-thumbprint marker — RavenDB will not " +
                    "trust the admin cert and the secure store may fail to authenticate (appliance can " +
                    "hang in Restarting).");
            return;
        }

        using var cert = X509CertificateLoader.LoadPkcs12FromFile(adminPfx, password: "");
        File.WriteAllText(Path.Combine(packagePath, "admin-thumbprint"), cert.Thumbprint);
    }

    private void RestartIntoSecureMode()
    {
        bootstrap.TryMarkRestarting();

        if (logger.IsInfoEnabled)
            logger.Info("Activation complete; restarting .NET host to bind the secure IDocumentStore.");
        if (logger.AuditEnabled)
            logger.Audit("ACTIVATION", "complete; restarting the host to bind the secure store", context: null);
        lifetime.StopApplication();
    }
}
