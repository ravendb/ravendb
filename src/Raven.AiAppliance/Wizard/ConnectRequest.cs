namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Body of <c>POST /api/setup/connect</c>. The wizard receives the source DB
/// type + raw connection string from the Admin and forwards it (after upserting
/// a probe SqlConnectionString) to <c>POST /admin/cdc-sink/verify</c>.
/// </summary>
public sealed record ConnectRequest(
    string Provider,
    string ConnectionString,
    List<string>? TableNames = null);
