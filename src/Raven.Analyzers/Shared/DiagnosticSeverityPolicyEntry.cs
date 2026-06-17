using Microsoft.CodeAnalysis;

namespace Raven.Analyzers.Shared
{
    /// <summary>
    /// Captures the graduated-severity policy for a single diagnostic: the version it was
    /// introduced in, the severity it is destined to reach, and the severity that is currently
    /// effective for the product version this assembly was built against.
    /// </summary>
    internal sealed class DiagnosticSeverityPolicyEntry
    {
        public DiagnosticSeverityPolicyEntry(
            string id,
            string introducedAt,
            DiagnosticSeverity destinationSeverity,
            DiagnosticSeverity effectiveSeverity)
        {
            Id = id;
            IntroducedAt = introducedAt;
            DestinationSeverity = destinationSeverity;
            EffectiveSeverity = effectiveSeverity;
        }

        /// <summary>The diagnostic id, e.g. <c>RVN012</c>.</summary>
        public string Id { get; }

        /// <summary>The version the rule first shipped in, e.g. <c>"7.2"</c> or <c>"7.2.5"</c>.</summary>
        public string IntroducedAt { get; }

        /// <summary>The severity the rule is promoted to once the product moves past the introducing release.</summary>
        public DiagnosticSeverity DestinationSeverity { get; }

        /// <summary>The severity currently in effect for the built product version.</summary>
        public DiagnosticSeverity EffectiveSeverity { get; }
    }
}
