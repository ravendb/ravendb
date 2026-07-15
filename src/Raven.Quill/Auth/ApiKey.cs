namespace Raven.Quill.Auth;

internal sealed class ApiKey
{
    internal const string IdPrefix = "api-keys/";

    internal const string PrimaryId = IdPrefix + "primary";

    public string? Id { get; set; }

    public string Label { get; set; } = "";

    public string Salt { get; set; } = "";

    public string Hash { get; set; } = "";

    public bool Revoked { get; set; }

    public DateTime CreatedAt { get; set; }
}
