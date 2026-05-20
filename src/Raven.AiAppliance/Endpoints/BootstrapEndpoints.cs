using System.IO.Compression;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
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
    /// First-run activation. Fetches the setup-package zip from the configured
    /// license upstream and unpacks it into <see cref="ApplianceOptions.SetupPackagePath"/>,
    /// then flips <see cref="IBootstrapState"/> to Ready so the wizard endpoints
    /// become live.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>What this does today</b> — the upstream at
    /// <see cref="ApplianceOptions.LicenseApiUrl"/> returns a pre-baked setup-package
    /// zip (mocked in tests via <c>MockLicenseApi</c>; in the demo the operator drops
    /// the zip at the path in <c>APPLIANCE_E2E_SETUP_PACKAGE_PATH</c>). The appliance
    /// unpacks it on disk and trusts whatever certs/license/settings are inside.
    /// </para>
    /// <para>
    /// <b>Production gap (Phase 5 / Stage A)</b> — the upstream must dynamically
    /// construct a per-license-key setup package containing all of the following.
    /// None of this is implemented yet on the appliance-builder website (Track J);
    /// the demo bridges it with a hand-rolled zip.
    /// </para>
    /// <list type="number">
    ///   <item><description>
    ///     DNS registration of <c>&lt;appname&gt;.ravendb.run</c> (subdomain + A record)
    ///     so the cert + URLs resolve before the appliance boots.
    ///   </description></item>
    ///   <item><description>
    ///     Wildcard certificate from Let's Encrypt for <c>*.&lt;appname&gt;.ravendb.run</c>
    ///     via ACME-DNS challenge, written into the zip as
    ///     <c>cluster.server.certificate.&lt;domain&gt;.pfx</c> +
    ///     <c>admin.client.certificate.&lt;domain&gt;.pfx</c>. RavenDB's Setup Wizard
    ///     already produces this layout — see
    ///     <c>Raven.Server/Commercial/LetsEncrypt/SettingsZipFileHelper.cs</c>.
    ///   </description></item>
    ///   <item><description>
    ///     URL mappings for the dashboard and RavenDB Studio (sidecar
    ///     <c>appliance.json</c>) so the appliance reads them at boot.
    ///   </description></item>
    ///   <item><description>
    ///     The signed <c>license.json</c> so RavenDB picks it up at start (no separate
    ///     in-process redeem step on the RavenDB side).
    ///   </description></item>
    ///   <item><description>
    ///     A pre-generated RavenDB <c>settings.json</c> binding on
    ///     <c>&lt;appname&gt;.ravendb.run:443</c> with TLS + Security.Certificate.Path
    ///     pointing at the unpacked PFX. The current
    ///     <c>docker/ai-appliance/ravendb-settings.json</c> is a demo placeholder
    ///     (Unsecured, public <c>0.0.0.0:8080</c>).
    ///   </description></item>
    /// </list>
    /// <para>
    /// <b>Production gap (appliance-side)</b> — once the package is on disk this
    /// method just flips bootstrap to Ready. A real activation needs to additionally:
    /// </para>
    /// <list type="number">
    ///   <item><description>
    ///     Hot-reload Kestrel's TLS cert from the new PFX so the appliance's
    ///     <c>:443</c> listener starts serving the right chain.
    ///   </description></item>
    ///   <item><description>
    ///     Restart / rebind the in-process RavenDB so it picks up the new
    ///     <c>settings.json</c>, cert path, and license. RavenDB doesn't re-read
    ///     settings on the fly today.
    ///   </description></item>
    ///   <item><description>
    ///     Re-create the appliance's <see cref="Raven.Client.Documents.IDocumentStore"/>
    ///     against the now-secured RavenDB URL with the admin client cert.
    ///   </description></item>
    /// </list>
    /// <para>
    /// The E2E test sidesteps both gaps: <c>WebApplicationFactory</c> injects an
    /// <c>IDocumentStore</c> built by <c>RavenTestBase</c>, and the bundled demo zip
    /// carries pre-arranged certs the operator already trusts. Don't read the
    /// happy-path E2E pass as evidence the production gaps are closed.
    /// </para>
    /// </remarks>
    private static async Task<IResult> RedeemLicenseAsync(
        RedeemLicenseRequest body,
        IBootstrapState bootstrap,
        IOptions<ApplianceOptions> options,
        IHttpClientFactory httpClientFactory,
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
            using var http = httpClientFactory.CreateClient();
            var url = $"{opts.LicenseApiUrl.TrimEnd('/')}/licenses/{Uri.EscapeDataString(body.LicenseKey)}";
            using var upstream = await http.GetAsync(url, ct);
            if (!upstream.IsSuccessStatusCode)
            {
                var msg = $"license api returned {(int)upstream.StatusCode} {upstream.ReasonPhrase}";
                logger.LogWarning("License redemption failed: {Reason}", msg);
                bootstrap.MarkFailed(msg);
                return Results.Problem(detail: msg, statusCode: (int)upstream.StatusCode);
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
                await using (var upstreamStream = await upstream.Content.ReadAsStreamAsync(ct))
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

            // Production gap (appliance-side): MarkReady() here only flips the
            // bootstrap state machine. Hot-reloading Kestrel's TLS cert from the
            // new PFX, restarting in-process RavenDB against the new
            // settings.json, and re-creating the IDocumentStore against the
            // now-secured URL are all deferred — see the <remarks> on this
            // method for the full enumeration.
            bootstrap.MarkReady();
            return Results.Ok(new { state = "ready" });
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
