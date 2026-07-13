namespace Raven.Quill.AiHelper;

/// <summary>
/// Shared copy helper for <see cref="ILicenseClient"/> implementations. Streams the zip to its
/// destination with a hard size cap so a misbehaving / malicious upstream returning a huge archive
/// can't OOM the appliance or fill the disk. Real setup packages are &lt;100 KB; the 32 MB cap
/// gives ~300x headroom.
/// </summary>
internal static class SetupPackageDownload
{
    public const long MaxSetupPackageBytes = 32L * 1024 * 1024;

    public static async Task CopyCappedAsync(Stream source, Stream destination, CancellationToken ct)
    {
        var buffer = new byte[81920];
        long total = 0;
        int read;
        while ((read = await source.ReadAsync(buffer, ct)) > 0)
        {
            // Track bytes explicitly rather than reading destination.Position — the cap is on what we
            // write this call, and Position throws on non-seekable destinations.
            total += read;
            if (total > MaxSetupPackageBytes)
                throw new InvalidOperationException(
                    $"setup package exceeds the {MaxSetupPackageBytes:N0} byte cap; aborting download.");
            await destination.WriteAsync(buffer.AsMemory(0, read), ct);
        }
    }
}
