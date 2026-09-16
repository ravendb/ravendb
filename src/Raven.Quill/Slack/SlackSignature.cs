using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Raven.Quill.Slack;

internal static class SlackSignature
{
    internal const string SignatureHeaderName = "X-Slack-Signature";
    internal const string TimestampHeaderName = "X-Slack-Request-Timestamp";

    private const string Prefix = "v0=";

    private const long MaxUnixSeconds = 253_402_300_799;

    internal static bool IsValid(
        ReadOnlySpan<byte> rawBody,
        string signingSecret,
        string? signatureHeader,
        string? timestampHeader,
        TimeSpan tolerance,
        DateTime utcNow)
    {
        if (signatureHeader is null || signatureHeader.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase) == false)
            return false;

        if (long.TryParse(timestampHeader, NumberStyles.None, CultureInfo.InvariantCulture, out var unixSeconds) == false ||
            unixSeconds > MaxUnixSeconds)
            return false;

        var skew = utcNow - DateTimeOffset.FromUnixTimeSeconds(unixSeconds).UtcDateTime;
        if (skew > tolerance || skew < -tolerance)
            return false;

        var hex = signatureHeader.AsSpan(Prefix.Length);
        Span<byte> provided = stackalloc byte[32];
        if (hex.Length != 64 ||
            Convert.FromHexString(hex, provided, out _, out var written) != OperationStatus.Done ||
            written != 32)
            return false;

        using var hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, Encoding.UTF8.GetBytes(signingSecret));
        hmac.AppendData(Encoding.ASCII.GetBytes($"v0:{timestampHeader}:"));
        hmac.AppendData(rawBody);
        Span<byte> expected = stackalloc byte[32];
        hmac.GetHashAndReset(expected);
        return CryptographicOperations.FixedTimeEquals(provided, expected);
    }
}
