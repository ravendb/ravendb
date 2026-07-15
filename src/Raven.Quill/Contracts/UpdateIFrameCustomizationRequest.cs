using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record UpdateIFrameCustomizationRequest(IFrameStyle? Style, string? Css);
