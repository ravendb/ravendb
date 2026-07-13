namespace Raven.Quill.Contracts;

/// <summary>
/// A self-contained HTML document mirroring the web-widget (iFrame) embed — the same base
/// styles and markup the live embed page uses, with sample chat bubbles and an empty
/// <c>&lt;style id="raven-custom"&gt;</c> slot, but no live chat script. The dashboard renders it
/// in a same-origin <c>srcdoc</c> iframe and injects the editor's CSS into the slot so the
/// customization preview matches production without the cross-origin restriction of framing
/// the real <c>public.*</c> page.
/// </summary>
public sealed record IFramePreviewResponse(string Html);
