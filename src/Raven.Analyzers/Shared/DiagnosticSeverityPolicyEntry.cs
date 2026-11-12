using Microsoft.CodeAnalysis;

namespace Raven.Analyzers.Shared
{
    /// <summary>
    /// Captures the graduated-severity policy for a single diagnostic: the version it was introduced
    /// in (<paramref name="IntroducedAt"/>), the severity it is destined to reach
    /// (<paramref name="DestinationSeverity"/>), and the severity currently effective for the product
    /// version this assembly was built against (<paramref name="EffectiveSeverity"/>).
    /// </summary>
    public sealed record DiagnosticSeverityPolicyEntry(
        string Id,
        string IntroducedAt,
        DiagnosticSeverity DestinationSeverity,
        DiagnosticSeverity EffectiveSeverity);
}
