namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// The appliance license forwarded to the internal AI service. The shape mirrors
/// <c>Raven.Server.Commercial.License.ToJson()</c>: <c>{ Id, Name, Keys }</c>, so
/// api.ravendb.net verifies it identically to the Studio AI-assistant path.
/// Parsed from the redeemed <c>license.json</c>.
/// </summary>
public sealed class ApplianceLicense
{
    public string? Id { get; set; }

    public string? Name { get; set; }

    public List<string>? Keys { get; set; }
}
