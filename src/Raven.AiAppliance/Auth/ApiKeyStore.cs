using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;
using Raven.Client.Documents;

namespace Raven.AiAppliance.Auth;

/// <summary>
/// Validates operator API keys for the appliance admin surface (the <c>api.*</c> header credential
/// and the <c>dashboard.*</c> login). Beta model: a single key from <c>QUILL_API_KEY</c> is the
/// source of truth — its salted hash is computed in-memory (so validation never depends on RavenDB
/// being reachable) and hard-overwritten into the config DB as the durable record. Fail-closed:
/// when <c>QUILL_API_KEY</c> is unset, no key validates.
/// </summary>
public interface IApiKeyStore
{
    /// <summary>Constant-time validation of a presented key against the active key(s).</summary>
    Task<bool> ValidateAsync(string? presentedKey, CancellationToken ct);
}

public sealed class ApiKeyStore(
    IDocumentStore store,
    IOptions<ApplianceOptions> options,
    ILogger<ApiKeyStore> logger) : IApiKeyStore
{
    private const int SaltBytes = 16;

    private readonly SemaphoreSlim _seedLock = new(1, 1);
    private volatile Record[]? _keys;

    public async Task<bool> ValidateAsync(string? presentedKey, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(presentedKey))
            return false;

        var keys = _keys ?? await EnsureSeededAsync(ct);
        var presented = Encoding.UTF8.GetBytes(presentedKey);

        // No early return on the first match — keep the work uniform across the (tiny) key set.
        var match = false;
        foreach (var key in keys)
        {
            var hash = SHA256.HashData(Combine(key.Salt, presented));
            if (CryptographicOperations.FixedTimeEquals(hash, key.Hash))
                match = true;
        }

        return match;
    }

    private async Task<Record[]> EnsureSeededAsync(CancellationToken ct)
    {
        var current = _keys;
        if (current is not null)
            return current;

        await _seedLock.WaitAsync(ct);
        try
        {
            if (_keys is not null)
                return _keys;

            var envKey = options.Value.ApiKey;
            Record[] records;
            if (string.IsNullOrWhiteSpace(envKey))
            {
                logger.LogWarning(
                    "QUILL_API_KEY is not set; appliance admin authentication is disabled (fail-closed). " +
                    "Set QUILL_API_KEY to enable the dashboard and API.");
                records = [];
            }
            else
            {
                var salt = RandomNumberGenerator.GetBytes(SaltBytes);
                var hash = SHA256.HashData(Combine(salt, Encoding.UTF8.GetBytes(envKey)));
                records = [new Record(salt, hash)];
                await PersistPrimaryAsync(salt, hash, ct);
            }

            _keys = records;
            return records;
        }
        finally
        {
            _seedLock.Release();
        }
    }

    /// Best-effort hard-overwrite of the durable key record. Validation works from the in-memory
    /// snapshot regardless, so a transient DB hiccup here doesn't break auth.
    private async Task PersistPrimaryAsync(byte[] salt, byte[] hash, CancellationToken ct)
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
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to persist the API key hash to the config database.");
        }
    }

    private static byte[] Combine(byte[] a, byte[] b)
    {
        var buffer = new byte[a.Length + b.Length];
        Buffer.BlockCopy(a, 0, buffer, 0, a.Length);
        Buffer.BlockCopy(b, 0, buffer, a.Length, b.Length);
        return buffer;
    }

    private sealed record Record(byte[] Salt, byte[] Hash);
}
