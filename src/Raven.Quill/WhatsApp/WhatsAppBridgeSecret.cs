using System.Security.Cryptography;
using Microsoft.Extensions.Options;
using Raven.Quill.Hosting;

namespace Raven.Quill.WhatsApp;

internal interface IWhatsAppBridgeSecret
{
    ValueTask<string?> GetAsync(CancellationToken ct);
}

internal sealed class WhatsAppBridgeSecret(
    IOptions<ApplianceOptions> options,
    ILogger<WhatsAppBridgeSecret> logger) : IWhatsAppBridgeSecret
{
    internal const string TokenFileName = "bridge-token";

    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _cached;

    public async ValueTask<string?> GetAsync(CancellationToken ct)
    {
        if (_cached is not null)
            return _cached;

        await _gate.WaitAsync(ct);
        try
        {
            if (_cached is not null)
                return _cached;

            var opts = options.Value;
            if (string.IsNullOrEmpty(opts.WhatsAppBridgeToken) == false)
                return _cached = opts.WhatsAppBridgeToken;

            var path = Path.Combine(opts.WhatsAppDataDir, TokenFileName);
            try
            {
                if (File.Exists(path))
                {
                    var existing = (await File.ReadAllTextAsync(path, ct)).Trim();
                    if (existing.Length > 0)
                        return _cached = existing;
                }

                Directory.CreateDirectory(opts.WhatsAppDataDir);
                var token = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(32));
                await File.WriteAllTextAsync(path, token, ct);
                if (!OperatingSystem.IsWindows())
                    File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);

                logger.LogInformation("Minted whatsapp bridge token at {Path}", path);
                return _cached = token;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogWarning(
                    "Could not read or create the whatsapp bridge token at {Path}: {Error}", path, e.Message);
                return null;
            }
        }
        finally
        {
            _gate.Release();
        }
    }
}
