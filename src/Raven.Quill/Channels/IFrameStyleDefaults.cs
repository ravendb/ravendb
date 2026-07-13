namespace Raven.Quill.Channels;

/// <summary>
/// Per-app default web-widget (iFrame) embed styling. A single document per app database (id
/// <see cref="DocumentId"/>) holding the style applied to every iFrame channel that does not
/// choose its own <see cref="Channel.Style"/>. Lives in the app's own database alongside
/// the <see cref="Channel"/> docs so channel styling is co-located and resolves without
/// crossing into the config DB.
/// </summary>
internal sealed class IFrameStyleDefaults
{
    /// <summary>Fixed singleton id — one defaults doc per app database.</summary>
    internal const string DocumentId = "iframe-style-defaults/config";

    public string? Id { get; set; }

    /// <summary>The app-wide default style: a built-in preset or
    /// <see cref="IFrameStyle.Custom"/> (then <see cref="Css"/> applies). Null only on legacy
    /// docs written before this field existed — resolved by
    /// <see cref="IFrameStyleResolution.ForDefaults"/> (non-empty <see cref="Css"/> means
    /// Custom, otherwise <see cref="IFrameStyle.Light"/>).</summary>
    public IFrameStyle? Style { get; set; }

    /// <summary>Operator-authored default CSS, applied when <see cref="Style"/> is
    /// <see cref="IFrameStyle.Custom"/>.</summary>
    public string? Css { get; set; }

    /// <summary>UTC timestamp of the last edit.</summary>
    public DateTime? UpdatedAt { get; set; }
}
