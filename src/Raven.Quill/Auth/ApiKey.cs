namespace Raven.Quill.Auth;

internal sealed class ApiKey
{
    internal const string IdPrefix = "api-keys/";

    internal const string PrimaryId = IdPrefix + "primary";

    public string? Id { get; set; }

    public string Label { get; set; } = "";

    public string Salt { get; set; } = "";

    // fast salted hash is fine: high-entropy bearer secret, not a password
    public string Hash { get; set; } = "";

    public bool Revoked { get; set; }

    public DateTime CreatedAt { get; set; }
}
