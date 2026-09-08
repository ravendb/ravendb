using Raven.Quill.Hosting;

namespace Raven.Quill.Contracts;

public sealed record BootstrapStatusResponse(BootstrapPhase State, string? Reason = null);
