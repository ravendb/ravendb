using System;
using Microsoft.CodeAnalysis;

namespace Raven.Analyzers.Shared
{
    /// <summary>
    /// Computes the effective severity of a diagnostic from the version it was introduced in
    /// and the severity it should reach (<c>destinationSeverity</c>), relative to the product
    /// version this analyzer assembly was built against.
    ///
    /// A rule ships as <see cref="DiagnosticSeverity.Info"/> in the release that introduces it and
    /// is promoted to its destination severity once the product has moved past that release. The
    /// promotion cadence follows the precision of the introduced version string: <c>"7.2"</c> is
    /// compared at major.minor granularity (promotes at <c>7.3.0</c>), while <c>"7.2.5"</c> is
    /// compared at major.minor.patch granularity (promotes at <c>7.2.6</c>).
    ///
    /// The <see cref="ProductVersion"/> constant is baked in at build time by the
    /// <c>GenerateRavenProductVersion</c> target in <c>Raven.Analyzers.csproj</c>, which reads the
    /// canonical <c>AssemblyVersion</c> from <c>src/CommonAssemblyInfo.cs</c>.
    /// </summary>
    public static partial class SeverityPolicy
    {
        /// <summary>
        /// Resolves the effective severity against the baked-in <see cref="ProductVersion"/>.
        /// </summary>
        public static DiagnosticSeverity Resolve(string introducedAt, DiagnosticSeverity destinationSeverity)
            => Resolve(ProductVersion, introducedAt, destinationSeverity);

        /// <summary>
        /// Pure resolver: returns <paramref name="destinationSeverity"/> if <paramref name="productVersion"/>
        /// has moved past <paramref name="introducedAt"/> (compared at the granularity of the
        /// introduced version), otherwise <see cref="DiagnosticSeverity.Info"/>.
        /// </summary>
        public static DiagnosticSeverity Resolve(string productVersion, string introducedAt, DiagnosticSeverity destinationSeverity)
            => HasMovedPast(productVersion, introducedAt)
                ? destinationSeverity
                : DiagnosticSeverity.Info;

        private static bool HasMovedPast(string productVersion, string introducedAt)
        {
            int[] product = Parse(productVersion);
            int[] introduced = Parse(introducedAt);

            // Compare only as many components as the introduced version declares: "7.2" compares
            // major.minor; "7.2.5" compares major.minor.patch.
            for (int i = 0; i < introduced.Length; i++)
            {
                int p = i < product.Length ? product[i] : 0;
                int n = introduced[i];

                if (p > n)
                    return true;
                if (p < n)
                    return false;
            }

            // Equal at the compared granularity: still the introducing release.
            return false;
        }

        private static int[] Parse(string version)
        {
            if (string.IsNullOrWhiteSpace(version))
                return Array.Empty<int>();

            string[] parts = version.Split('.');
            int[] result = new int[parts.Length];
            for (int i = 0; i < parts.Length; i++)
                result[i] = int.TryParse(parts[i], out int value) ? value : 0;

            return result;
        }
    }
}
