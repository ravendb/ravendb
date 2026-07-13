namespace Raven.Quill.Contracts;

/// <summary>
/// Channel-edit input. All fields optional — only the supplied ones are
/// applied (PATCH semantics over a PUT verb). <c>Type</c> and <c>AgentId</c>
/// are intentionally not editable: they define the channel's binding tuple
/// (<c>channel-bindings/{slug}/{type}/{agentId}</c>), so changing them is a
/// delete + re-create, not an edit.
/// </summary>
/// <param name="DisplayName">New operator-friendly label.</param>
/// <param name="AllowedOrigins">Replacement allowed-origins list (validated +
/// normalized like create).</param>
/// <param name="Enabled">Pause (<c>false</c>) or resume (<c>true</c>) the channel.</param>
public sealed record UpdateChannelRequest(
    string? DisplayName,
    string[]? AllowedOrigins,
    bool? Enabled);
