using Raven.AiAppliance.Hosting;

namespace Raven.AiAppliance.Contracts;

public sealed record BootstrapRedeemConflictResponse(string Error, BootstrapPhase State);
