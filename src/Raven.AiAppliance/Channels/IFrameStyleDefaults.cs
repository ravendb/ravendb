namespace Raven.AiAppliance.Channels;

/// <summary>
/// Per-app default web-widget (iFrame) embed styling. A single document per app database (id
/// <see cref="DocumentId"/>) holding the CSS applied to every iFrame channel that does not
/// define its own <see cref="Channel.CustomCss"/>. Lives in the app's own database alongside
/// the <see cref="Channel"/> docs so channel styling is co-located and resolves without
/// crossing into the config DB.
/// </summary>
internal sealed class IFrameStyleDefaults
{
    /// <summary>Fixed singleton id — one defaults doc per app database.</summary>
    internal const string DocumentId = "iframe-style-defaults/config";

    public string? Id { get; set; }

    /// <summary>Operator-authored default CSS. Null or empty means "no default"; channels with
    /// no <see cref="Channel.CustomCss"/> then render only the widget's base styles.</summary>
    public string? Css { get; set; }

    /// <summary>UTC timestamp of the last edit.</summary>
    public DateTime? UpdatedAt { get; set; }
}
