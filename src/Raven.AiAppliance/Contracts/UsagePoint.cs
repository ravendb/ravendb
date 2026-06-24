namespace Raven.AiAppliance.Contracts;

/// <summary>
/// One hourly point of the global usage series — the prototype's
/// <c>UsagePoint</c> (mock-api.ts). <paramref name="Invocations"/> = agent turns
/// (messages) in the hour starting at <paramref name="Timestamp"/> (UTC);
/// <paramref name="Tokens"/> = summed token usage in that hour.
/// </summary>
public sealed record UsagePoint(DateTime Timestamp, long Invocations, long Tokens);
