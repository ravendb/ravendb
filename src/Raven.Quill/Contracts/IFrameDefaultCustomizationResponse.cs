using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record IFrameDefaultCustomizationResponse(IFrameStyle Style, string? Css);
