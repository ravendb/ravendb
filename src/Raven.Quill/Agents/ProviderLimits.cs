namespace Raven.Quill.Agents;

internal static class ProviderLimits
{
    internal const int MaxRateLimitedRetries = 2;

    internal static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(1);
}
