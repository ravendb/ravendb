using System.Diagnostics;
using System.IO.Compression;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;

namespace Raven.AiAppliance.Endpoints;

/// First-run flow endpoints. Live regardless of <see cref="IBootstrapState"/> —
/// they're the only way out of <see cref="BootstrapPhase.NeedsActivation"/>.
public static class BootstrapEndpoints
{
    public sealed record RedeemLicenseRequest(string LicenseKey);

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/bootstrap");
        group.MapGet("/status", GetStatus);
        group.MapPost("/redeem-license", RedeemLicenseAsync);
    }

    private static IResult GetStatus(IBootstrapState state) =>
        Results.Ok(new
        {
            state = state.Phase.ToWire(),
            reason = state.Reason,
        });

    /// <summary>
    /// First-run activation. Acquires the setup-package zip (from
    /// <see cref="ApplianceOptions.SetupPackageZipPath"/> in the demo, or from the
    /// license upstream in production), unpacks it into
    /// <see cref="ApplianceOptions.SetupPackagePath"/>, signals s6 to restart
    /// RavenDB in secure mode, and triggers a .NET host restart so the
    /// secure <see cref="Raven.Client.Documents.IDocumentStore"/> is rebuilt
    /// against the package's <c>PublicServerUrl</c> + admin client cert.
    /// Response carries <c>{state: "restarting"}</c>; the frontend polls
    /// <c>/api/bootstrap/status</c> until it sees <c>ready</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Demo vs. production source of the setup package.</b> The 8-week demo
    /// mounts a pre-built zip at the path in <c>RAVEN_AI_SETUP_PACKAGE_ZIP</c>
    /// (set by the Dockerfile, populated by <c>up.ps1</c> from
    /// <c>$env:APPLIANCE_E2E_SETUP_PACKAGE_PATH</c>). When the file exists the
    /// HTTP call is skipped and the license-key value is logged but otherwise
    /// unused. Production will POST the key to <see cref="ApplianceOptions.LicenseApiUrl"/>
    /// and receive only <c>license.json</c> + <c>app-name</c>; the appliance
    /// then runs LE provisioning locally — DNS registration, ACME challenge,
    /// cert generation, <c>settings.json</c> write — before reaching the same
    /// restart sequence below.
    /// </para>
    /// <para>
    /// <b>Remaining gap (Kestrel cert).</b> RavenDB picks up the new
    /// <c>settings.json</c> via the s6 restart and the appliance reconnects
    /// over TLS with the admin cert. Kestrel itself still listens on plain
    /// HTTP <c>:5000</c>; serving the dashboard on <c>:443</c> with the LE cert
    /// is a separate follow-up (out of scope for this slice).
    /// </para>
    /// </remarks>
    private static async Task<IResult> RedeemLicenseAsync(
        RedeemLicenseRequest body,
        IBootstrapState bootstrap,
        IOptions<ApplianceOptions> options,
        IHttpClientFactory httpClientFactory,
        IHostApplicationLifetime lifetime,
        ILogger<BootstrapLicenseLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.LicenseKey))
            return Results.BadRequest(new { error = "licenseKey is required" });

        var opts = options.Value;

        // CAS guard against operator double-click — two concurrent POSTs
        // would otherwise both fetch + extract into /setup/, interleaving
        // zips and racing the IDocumentStore reload.
        if (!bootstrap.TryMarkRedeeming())
        {
            return Results.Conflict(new
            {
                error = "redemption already in progress or completed",
                state = bootstrap.Phase.ToWire(),
            });
        }

        try
        {
            // Demo path: a pre-baked zip mounted into the container short-circuits
            // the HTTP call. The license-key value is logged but otherwise unused;
            // production hits the license API to fetch license.json + app-name
            // and runs LE provisioning locally (no zip involved).
            Stream upstreamStream;
            HttpResponseMessage? upstreamResponse = null;
            HttpClient? http = null;
            if (!string.IsNullOrEmpty(opts.SetupPackageZipPath) && File.Exists(opts.SetupPackageZipPath))
            {
                logger.LogInformation(
                    "Reading setup package from local path {Path} (demo mode).",
                    opts.SetupPackageZipPath);
                upstreamStream = File.OpenRead(opts.SetupPackageZipPath);
            }
            else
            {
                http = httpClientFactory.CreateClient();
                var url = $"{opts.LicenseApiUrl.TrimEnd('/')}/licenses/{Uri.EscapeDataString(body.LicenseKey)}";
                upstreamResponse = await http.GetAsync(url, ct);
                if (!upstreamResponse.IsSuccessStatusCode)
                {
                    var msg = $"license api returned {(int)upstreamResponse.StatusCode} {upstreamResponse.ReasonPhrase}";
                    logger.LogWarning("License redemption failed: {Reason}", msg);
                    bootstrap.MarkFailed(msg);
                    upstreamResponse.Dispose();
                    http.Dispose();
                    return Results.Problem(detail: msg, statusCode: (int)upstreamResponse.StatusCode);
                }
                upstreamStream = await upstreamResponse.Content.ReadAsStreamAsync(ct);
            }

            // Stream the zip to a temp file (capped) instead of buffering in
            // memory — a misbehaving / malicious upstream returning a huge
            // archive would otherwise OOM the appliance. The cap also bounds
            // disk usage. Real production setup packages are <100 KB; a 32 MB
            // cap gives 300× headroom.
            const long MaxSetupPackageBytes = 32L * 1024 * 1024;
            var tempZipPath = Path.Combine(Path.GetTempPath(), $"setup-package-{Guid.NewGuid():N}.zip");
            long downloadedBytes;
            try
            {
                await using (upstreamStream)
                await using (var tempFile = File.Create(tempZipPath))
                {
                    var buffer = new byte[81920];
                    int read;
                    while ((read = await upstreamStream.ReadAsync(buffer, ct)) > 0)
                    {
                        if (tempFile.Position + read > MaxSetupPackageBytes)
                            throw new InvalidOperationException(
                                $"setup package exceeds the {MaxSetupPackageBytes:N0} byte cap; aborting download.");
                        await tempFile.WriteAsync(buffer.AsMemory(0, read), ct);
                    }
                    downloadedBytes = tempFile.Position;
                }

                Directory.CreateDirectory(opts.SetupPackagePath);
                // Zip Slip protection: ZipFile.ExtractToDirectory in .NET 9+
                // (we target net10.0) resolves each entry's destination via
                // Path.GetFullPath against the target dir and throws IOException
                // if the resolved path escapes the destination — so `../` and
                // absolute-path entries from a hostile / corrupted zip are
                // rejected before any file is written. No manual entry-name
                // validation needed.
                ZipFile.ExtractToDirectory(tempZipPath, opts.SetupPackagePath, overwriteFiles: true);
            }
            finally
            {
                upstreamResponse?.Dispose();
                http?.Dispose();

                // Best-effort cleanup. Narrow to the two exceptions File.Delete
                // can legitimately throw on a stray temp file (the path is
                // ours, no malformed-path risks) — anything else is unexpected
                // and worth letting bubble.
                try { File.Delete(tempZipPath); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    logger.LogDebug(ex, "Failed to delete temp zip {Path}", tempZipPath);
                }
            }

            logger.LogInformation(
                "Setup package redeemed and unpacked to {Path} ({Bytes} bytes).",
                opts.SetupPackagePath, downloadedBytes);

            bootstrap.TryMarkRestarting();

            // Restart RavenDB so it re-reads settings.json. The s6 run script
            // copies A/settings.json from the setup package over the baked-in
            // unsecured config on every start; RavenDB itself does not hot-
            // reload its settings. Linux-only — Windows / test hosts (no s6)
            // skip this and rely on the .NET host restart only, which is fine
            // for tests since they inject the IDocumentStore directly.
            if (OperatingSystem.IsLinux())
            {
                try
                {
                    using var s6 = Process.Start(new ProcessStartInfo("s6-svc", "-r /run/service/01-ravendb")
                    {
                        UseShellExecute = false,
                    });
                    s6?.WaitForExit(5000);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex,
                        "Could not signal s6 to restart RavenDB; relying on .NET host restart only.");
                }
            }

            logger.LogInformation(
                "Activation complete; restarting .NET host to bind secure IDocumentStore.");

            // Brief delay so the HTTP response flushes to the client before
            // Kestrel shuts down — otherwise the browser sees a connection-
            // reset and can't read the `restarting` state from the body it's
            // about to render the spinner from.
            _ = Task.Delay(500, CancellationToken.None)
                .ContinueWith(_ => lifetime.StopApplication(), TaskScheduler.Default);

            return Results.Ok(new { state = "restarting" });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            bootstrap.MarkFailed("redemption cancelled");
            throw;
        }
        catch (InvalidDataException ex)
        {
            // ZipFile.ExtractToDirectory throws InvalidDataException for a
            // corrupt / non-zip payload. That's an upstream-bad-bytes problem,
            // not an appliance failure — surface as 502 Bad Gateway so the
            // caller can distinguish "license server returned garbage" from
            // "the appliance itself is broken".
            var detail = $"invalid setup package: {ex.Message}";
            logger.LogWarning(ex, "License redemption: upstream returned an invalid zip.");
            bootstrap.MarkFailed(detail);
            return Results.Problem(detail: detail, statusCode: StatusCodes.Status502BadGateway);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "License redemption threw.");
            bootstrap.MarkFailed(ex.Message);
            return Results.Problem(detail: ex.Message, statusCode: 500);
        }
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class BootstrapLicenseLogger;
}
