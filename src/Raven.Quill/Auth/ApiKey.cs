namespace Raven.Quill.Auth;

public sealed class ApiKey
{
    public const string IdPrefix = "api-keys/";

    public const string PrimaryId = IdPrefix + "primary";

    public string? Id { get; set; }

    public string Label { get; set; } = "";

    public string Salt { get; set; } = "";

    public string Hash { get; set; } = "";

    public bool Revoked { get; set; }

    public DateTime CreatedAt { get; set; }
}
