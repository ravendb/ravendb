namespace Raven.AiAppliance.Channels;

/// <summary>
/// The kind of channel an agent is exposed through. Persisted on the
/// <see cref="Channel"/> doc (RavenDB stores enums as their string name by
/// default, so the on-disk value is <c>"IFrame"</c>) and used to dispatch
/// per-type provisioning / editing / deletion in
/// <see cref="Endpoints.ChannelsEndpoints"/>.
///
/// Only <see cref="IFrame"/> is implemented in the 8-week demo;
/// <see cref="Telegram"/> (RavenDB-26631) and <see cref="WhatsApp"/> are the
/// seams the per-type switch fills next.
/// </summary>
public enum ChannelType
{
    IFrame,
    Telegram,
    WhatsApp,
}
