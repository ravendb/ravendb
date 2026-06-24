namespace Raven.AiAppliance.Contracts;

/// <summary>
/// An activity-log event for the CDC page's "Events" tab — the prototype's
/// <c>ActivityEvent</c>. DEFERRED: there is no event-log source yet, so the
/// endpoint returns an empty feed (a real audit log is a separate ticket).
/// </summary>
public sealed record ActivityEventDto(string Id, string AppId, string Type, string Message, DateTime Timestamp);
