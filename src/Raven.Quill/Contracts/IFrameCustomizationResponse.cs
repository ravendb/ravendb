using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record IFrameCustomizationResponse(
    IFrameStyle? Style,
    string? Css,
    IFrameStyle DefaultStyle,
    string? DefaultCss);
