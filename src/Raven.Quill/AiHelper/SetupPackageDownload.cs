namespace Raven.Quill.AiHelper;

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
            total += read;
            if (total > MaxSetupPackageBytes)
                throw new InvalidOperationException(
                    $"setup package exceeds the {MaxSetupPackageBytes:N0} byte cap; aborting download.");
            await destination.WriteAsync(buffer.AsMemory(0, read), ct);
        }
    }
}
