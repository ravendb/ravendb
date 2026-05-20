namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Body of <c>POST /api/setup/provision</c>. The wizard already has the source
/// connection (Connect/Discover) and the CDC mapping (Map) stashed on
/// wizard-state; Provision only needs to know what to call the new app.
/// </summary>
internal sealed record ProvisionRequest(string AppName);
