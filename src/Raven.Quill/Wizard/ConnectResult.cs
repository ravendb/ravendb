namespace Raven.Quill.Wizard;

using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

public sealed class ConnectResult
{
    [SetsRequiredMembers]
    public ConnectResult()
    {
    }

    [JsonRequired]
    public required bool Success { get; set; }

    [JsonRequired]
    public required List<string> Errors { get; set; } = new();
}
