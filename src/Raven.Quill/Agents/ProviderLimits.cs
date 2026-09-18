namespace Raven.Quill.Agents;

internal static class ProviderLimits
{
    internal const int MaxRateLimitedRetries = 2;

    internal static readonly TimeSpan MinRetryDelay = TimeSpan.FromSeconds(1);

    internal static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(20);
}
