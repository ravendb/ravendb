namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Body of <c>POST /api/setup/connect</c>. The wizard receives the source DB
/// type + raw connection string from the Admin and runs a plain reachability
/// probe. Schema enumeration is <see cref="DiscoverRequest"/>'s job.
/// </summary>
public sealed record ConnectRequest(
    string Provider,
    string ConnectionString);
