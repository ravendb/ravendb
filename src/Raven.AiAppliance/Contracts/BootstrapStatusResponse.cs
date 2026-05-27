using Raven.AiAppliance.Hosting;

namespace Raven.AiAppliance.Contracts;

public sealed record BootstrapStatusResponse(BootstrapPhase State, string? Reason = null);
