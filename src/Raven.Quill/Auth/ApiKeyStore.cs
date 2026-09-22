using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using SessionOptions = Raven.Client.Documents.Session.SessionOptions;
using Raven.Client.Documents;
using Raven.Quill.Hosting;
using Raven.Quill.Logging;
using Raven.Server.Logging;

namespace Raven.Quill.Auth;

public interface IApiKeyStore
{
    Task<bool> ValidateAsync(string? presentedKey, CancellationToken ct);
}

public sealed class ApiKeyStore(
    IDocumentStore store,
    IOptions<ApplianceOptions> options,
    QuillLogger<ApiKeyStore> logger) : IApiKeyStore
{
    private const string PrimaryKeyId = "primary";

    private const int SaltBytes = 16;
    private const int MaxKeyIdLength = 64;
    private const int MinRecommendedApiKeyLength = 16;

    private static readonly Record Decoy = new(RandomNumberGenerator.GetBytes(SaltBytes), RandomNumberGenerator.GetBytes(32));

    private readonly SemaphoreSlim _seedLock = new(1, 1);
    private readonly ConcurrentDictionary<string, CacheEntry> _cache = new(StringComparer.OrdinalIgnoreCase);
    private volatile bool _seeded;

    public async Task<bool> ValidateAsync(string? presentedKey, CancellationToken ct)
    {
        if (TryParse(presentedKey, out var keyId, out var secret) == false)
            return false;

        if (_seeded == false)
        {
            await SeedPrimaryAsync(ct);
            if (_seeded == false && string.Equals(keyId, PrimaryKeyId, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        var record = await LoadAsync(keyId, ct);
        var target = record ?? Decoy;
        var hash = HashSecret(target.Salt, secret);
        var equal = CryptographicOperations.FixedTimeEquals(hash, target.Hash);
        return equal && record is not null;
    }

    private static bool TryParse(string? presentedKey, out string keyId, out string secret)
    {
        keyId = PrimaryKeyId;
        secret = "";
        if (string.IsNullOrEmpty(presentedKey))
            return false;

        var separator = presentedKey.IndexOf('/');
        if (separator >= 0)
        {
            keyId = presentedKey[..separator];
            secret = presentedKey[(separator + 1)..];
        }
        else
        {
            secret = presentedKey;
        }

        return secret.Length > 0 && IsValidKeyId(keyId);
    }

    private static bool IsValidKeyId(string keyId)
    {
        if (keyId.Length is 0 or > MaxKeyIdLength)
            return false;

        foreach (var c in keyId)
        {
            if (char.IsAsciiLetterOrDigit(c) == false && c != '-' && c != '_')
                return false;
        }

        return true;
    }

    private async Task<Record?> LoadAsync(string keyId, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        if (_cache.TryGetValue(keyId, out var cached) && cached.ExpiresAt > now)
            return cached.Record;

        ApiKey? doc;
        try
        {
            using var session = store.OpenAsyncSession(new SessionOptions { NoTracking = true });
            doc = await session.LoadAsync<ApiKey>(ApiKey.IdPrefix + keyId, ct);
        }
        catch (Exception ex)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(ex, $"Failed to load API key '{keyId}' from the config database.");
            return null;
        }

        if (doc is null)
        {
            _cache.TryRemove(keyId, out _);
            return null;
        }

        var record = doc.Revoked ? null : Decode(doc, keyId);
        _cache[keyId] = new CacheEntry(record, now + options.Value.ApiKeyCacheDuration);
        return record;
    }

    private Record? Decode(ApiKey doc, string keyId)
    {
        try
        {
            return new Record(Convert.FromBase64String(doc.Salt), Convert.FromBase64String(doc.Hash));
        }
        catch (FormatException ex)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(ex, $"API key '{keyId}' has malformed Base64 in Salt or Hash; treating it as revoked.");
            return null;
        }
    }

    private async Task SeedPrimaryAsync(CancellationToken ct)
    {
        await _seedLock.WaitAsync(ct);
        try
        {
            if (_seeded)
                return;

            var envKey = options.Value.ApiKey is { } configured ? StripKeyIdPrefix(configured) : null;
            if (string.IsNullOrWhiteSpace(envKey))
            {
                if (logger.IsWarnEnabled)
                    logger.Warn(
                        "QUILL_API_KEY is not set; the primary appliance API key is disabled (fail-closed). " +
                        "Set QUILL_API_KEY to enable the dashboard and API.");
                if (logger.AuditEnabled)
                    logger.Audit("AUTH",
                        "QUILL_API_KEY is not configured; the primary API key is disabled (fail-closed)",
                        context: null);
                _seeded = await RevokePrimaryAsync(ct);
                return;
            }

            if (envKey.Length < MinRecommendedApiKeyLength && logger.IsWarnEnabled)
                logger.Warn(
                    $"QUILL_API_KEY is shorter than {MinRecommendedApiKeyLength} characters; " +
                    "use a high-entropy key in production.");

            var salt = RandomNumberGenerator.GetBytes(SaltBytes);
            var hash = HashSecret(salt, envKey);
            _seeded = await PersistPrimaryAsync(salt, hash, ct);
        }
        finally
        {
            _seedLock.Release();
        }
    }

    private async Task<bool> PersistPrimaryAsync(byte[] salt, byte[] hash, CancellationToken ct)
    {
        try
        {
            using var session = store.OpenAsyncSession();
            var doc = new ApiKey
            {
                Label = "primary (QUILL_API_KEY)",
                Salt = Convert.ToBase64String(salt),
                Hash = Convert.ToBase64String(hash),
                Revoked = false,
                CreatedAt = DateTime.UtcNow,
            };
            await session.StoreAsync(doc, ApiKey.PrimaryId, ct);
            await session.SaveChangesAsync(ct);
            _cache.TryRemove(PrimaryKeyId, out _);
            return true;
        }
        catch (Exception ex)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(ex, "Failed to persist the primary API key hash to the config database.");
            return false;
        }
    }

    private async Task<bool> RevokePrimaryAsync(CancellationToken ct)
    {
        try
        {
            using var session = store.OpenAsyncSession();
            var doc = await session.LoadAsync<ApiKey>(ApiKey.PrimaryId, ct);
            if (doc is not null && doc.Revoked == false)
            {
                doc.Revoked = true;
                await session.SaveChangesAsync(ct);
            }

            _cache.TryRemove(PrimaryKeyId, out _);
            return true;
        }
        catch (Exception ex)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(ex, "Failed to revoke the primary API key in the config database.");
            return false;
        }
    }

    public static byte[] HashSecret(byte[] salt, string secret) =>
        SHA256.HashData(Combine(salt, Encoding.UTF8.GetBytes(secret)));

    private static string StripKeyIdPrefix(string key)
    {
        var separator = key.IndexOf('/');
        return separator < 0 ? key : key[(separator + 1)..];
    }

    private static byte[] Combine(byte[] a, byte[] b)
    {
        var buffer = new byte[a.Length + b.Length];
        Buffer.BlockCopy(a, 0, buffer, 0, a.Length);
        Buffer.BlockCopy(b, 0, buffer, a.Length, b.Length);
        return buffer;
    }

    private sealed record Record(byte[] Salt, byte[] Hash);

    private sealed record CacheEntry(Record? Record, DateTime ExpiresAt);
}
