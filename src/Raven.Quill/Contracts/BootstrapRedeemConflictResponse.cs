using Raven.Quill.Hosting;

namespace Raven.Quill.Contracts;

public sealed record BootstrapRedeemConflictResponse(string Error, BootstrapPhase State);
