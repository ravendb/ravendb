namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Body of <c>POST /api/setup/connect</c> and <c>POST /api/setup/discover</c>. The
/// wizard receives the source DB type + raw connection string from the Admin.
/// Connect runs a plain reachability probe; Discover enumerates and verifies the
/// schema in one call (the merged <c>/admin/cdc-sink/schema</c> eagerly verifies all
/// tables, so no table list is supplied).
/// </summary>
public sealed record ConnectRequest(
    string Provider,
    string ConnectionString);
