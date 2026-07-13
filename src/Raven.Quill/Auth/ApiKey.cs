namespace Raven.Quill.Auth;

/// <summary>
/// Persisted operator API-key record, stored in the config DB under <c>api-keys/{id}</c> (the
/// "dashboard API key hash" reserved in <see cref="ApplianceDatabases"/>). Only the salted hash is
/// stored — never the plaintext key.
/// </summary>
/// <remarks>
/// For beta there is a single env-seeded key (<see cref="PrimaryId"/>), hard-overwritten from
/// <c>QUILL_API_KEY</c> at first use so the env var is the source of truth. Generated multi-key
/// management (create/rotate/revoke from the dashboard + <c>docker exec</c>) is deferred post-beta.
/// </remarks>
internal sealed class ApiKey
{
    internal const string IdPrefix = "api-keys/";

    /// <summary>The single beta key, seeded from <c>QUILL_API_KEY</c>.</summary>
    internal const string PrimaryId = IdPrefix + "primary";

    public string? Id { get; set; }

    public string Label { get; set; } = "";

    /// <summary>Base64 random salt.</summary>
    public string Salt { get; set; } = "";

    /// <summary>Base64 <c>SHA-256(salt || key)</c>. The key is a high-entropy bearer secret, so a
    /// fast salted hash is appropriate here (this is not a low-entropy password).</summary>
    public string Hash { get; set; } = "";

    public bool Revoked { get; set; }

    public DateTime CreatedAt { get; set; }
}
