using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Xunit;

namespace AnalyzersTests.Policy
{
    /// <summary>
    /// Captures the graduated-severity policy of every RavenDB analyzer diagnostic and enforces it:
    ///
    /// - every diagnostic id declares a Start (introduced) version and a destination severity in code;
    /// - a rule ships as Info in the release that introduces it and is promoted to its destination
    ///   severity once the product moves past that release (see <c>SeverityPolicy</c>);
    /// - the captured table below must match what the code produces, so any new rule or any change to a
    ///   Start version / destination severity / currently-effective severity fails this test on purpose.
    ///
    /// The policy metadata lives on the internal <c>DiagnosticDescriptors.Policies</c> registry and is
    /// read here via reflection to avoid exposing internals across the strong-name signing boundary.
    /// </summary>
    public class DiagnosticSeverityPolicyTests
    {
        /// <summary>
        /// The captured, expected policy for every diagnostic. This is the snapshot the user asked for:
        /// it pins each rule's introduced version, destination severity, and the severity currently in
        /// effect for the built product version (7.2.x → every rule is still Info, promoting at 7.3.0).
        /// </summary>
        private static readonly IReadOnlyList<PolicyRow> Expected = new[]
        {
            new PolicyRow(DiagnosticIds.IndexMapAssignedOutsideCtor, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.QueryFilteringAfterProjection, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.DoubleProjectInto, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.IndexMissingMapAssignment, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.MultiMapIndexMissingAddMap, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.MultiMapIndexSingleAddMap, "7.2", DiagnosticSeverity.Info, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.QueryFieldNotIndexed, "7.2", DiagnosticSeverity.Info, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.QueryProjectionFieldNotRetrievable, "7.2", DiagnosticSeverity.Info, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.IndexUnsupportedMethodCall, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.QueryUnsupportedMethodCall, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.SubscriptionStoreOpenSession, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.SessionLazyBatching, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.QueryUnboundedResult, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
            new PolicyRow(DiagnosticIds.IndexFanOut, "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info),
        };

        [Fact]
        public void Every_DiagnosticId_Has_Exactly_One_Policy_Entry()
        {
            string[] ids = AllDiagnosticIds();
            List<PolicyRow> policies = ActualPolicies();

            // Every declared id must have a policy entry — a new rule that forgets to declare a Start
            // version / destination severity (i.e. is built without the Create factory) fails here.
            string[] missing = ids.Where(id => policies.All(p => p.Id != id)).ToArray();
            Assert.True(missing.Length == 0, $"Diagnostic ids without a severity policy entry: {string.Join(", ", missing)}");

            // No duplicates and no stray entries for ids that don't exist.
            string[] orphaned = policies.Select(p => p.Id).Where(id => !ids.Contains(id)).ToArray();
            Assert.True(orphaned.Length == 0, $"Policy entries for unknown diagnostic ids: {string.Join(", ", orphaned)}");

            string[] duplicated = policies.GroupBy(p => p.Id).Where(g => g.Count() > 1).Select(g => g.Key).ToArray();
            Assert.True(duplicated.Length == 0, $"Diagnostic ids with more than one policy entry: {string.Join(", ", duplicated)}");
        }

        [Fact]
        public void Captured_Severity_Policy_Matches_Code()
        {
            Dictionary<string, PolicyRow> actual = ActualPolicies().ToDictionary(p => p.Id);

            // Adding a rule, or changing a Start version / destination / currently-effective severity,
            // must be an intentional edit to the Expected table above.
            Assert.Equal(
                Expected.Select(r => r.ToString()).OrderBy(s => s, StringComparer.Ordinal),
                actual.Values.Select(r => r.ToString()).OrderBy(s => s, StringComparer.Ordinal));
        }

        [Fact]
        public void AnalyzerReleases_Severities_Are_In_Sync_With_Code()
        {
            Dictionary<string, DiagnosticSeverity> tracked = ReadAnalyzerReleaseSeverities();
            List<PolicyRow> policies = ActualPolicies();

            foreach (PolicyRow policy in policies)
            {
                Assert.True(tracked.ContainsKey(policy.Id),
                    $"{policy.Id} is not listed in any AnalyzerReleases file.");
                Assert.Equal(policy.Effective, tracked[policy.Id]);
            }

            string[] strayTracked = tracked.Keys.Where(id => policies.All(p => p.Id != id)).ToArray();
            Assert.True(strayTracked.Length == 0,
                $"AnalyzerReleases lists rules with no descriptor: {string.Join(", ", strayTracked)}");
        }

        [Theory]
        // Introduced at minor granularity ("7.2"): Info through 7.2.x, promotes at 7.3.0.
        [InlineData("7.2.5", "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info)]
        [InlineData("7.2.9", "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Info)]
        [InlineData("7.3.0", "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Warning)]
        [InlineData("8.0.0", "7.2", DiagnosticSeverity.Warning, DiagnosticSeverity.Warning)]
        // Introduced at patch granularity ("7.2.5"): Info only at 7.2.5, promotes at 7.2.6.
        [InlineData("7.2.5", "7.2.5", DiagnosticSeverity.Warning, DiagnosticSeverity.Info)]
        [InlineData("7.2.6", "7.2.5", DiagnosticSeverity.Warning, DiagnosticSeverity.Warning)]
        [InlineData("7.2.4", "7.2.5", DiagnosticSeverity.Warning, DiagnosticSeverity.Info)]
        // A destination of Info never escalates.
        [InlineData("9.0.0", "7.2", DiagnosticSeverity.Info, DiagnosticSeverity.Info)]
        // Any destination severity is honoured once promoted.
        [InlineData("7.4.0", "7.2", DiagnosticSeverity.Error, DiagnosticSeverity.Error)]
        public void Resolver_Computes_Effective_Severity(string productVersion, string introduced, DiagnosticSeverity destination, DiagnosticSeverity expected)
        {
            Assert.Equal(expected, Resolve(productVersion, introduced, destination));
        }

        // --- reflection helpers over the internal Raven.Analyzers types ---

        private static readonly Assembly AnalyzersAssembly = typeof(DiagnosticIds).Assembly;

        private static string[] AllDiagnosticIds() =>
            typeof(DiagnosticIds)
                .GetFields(BindingFlags.Public | BindingFlags.Static)
                .Where(f => f.IsLiteral && f.FieldType == typeof(string))
                .Select(f => (string)f.GetRawConstantValue()!)
                .ToArray();

        private static List<PolicyRow> ActualPolicies()
        {
            Type descriptors = AnalyzersAssembly.GetType("Raven.Analyzers.Shared.DiagnosticDescriptors", throwOnError: true)!;
            PropertyInfo policiesProperty = descriptors.GetProperty("Policies", BindingFlags.Public | BindingFlags.Static)!;
            var entries = (IEnumerable)policiesProperty.GetValue(null)!;

            var rows = new List<PolicyRow>();
            foreach (object entry in entries)
            {
                Type t = entry.GetType();
                string id = (string)t.GetProperty("Id")!.GetValue(entry)!;
                string introduced = (string)t.GetProperty("IntroducedAt")!.GetValue(entry)!;
                var destination = (DiagnosticSeverity)t.GetProperty("DestinationSeverity")!.GetValue(entry)!;
                var effective = (DiagnosticSeverity)t.GetProperty("EffectiveSeverity")!.GetValue(entry)!;
                rows.Add(new PolicyRow(id, introduced, destination, effective));
            }

            return rows;
        }

        private static DiagnosticSeverity Resolve(string productVersion, string introduced, DiagnosticSeverity destination)
        {
            Type policy = AnalyzersAssembly.GetType("Raven.Analyzers.Shared.SeverityPolicy", throwOnError: true)!;
            MethodInfo resolve = policy.GetMethod(
                "Resolve",
                BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: new[] { typeof(string), typeof(string), typeof(DiagnosticSeverity) },
                modifiers: null)!;

            return (DiagnosticSeverity)resolve.Invoke(null, new object[] { productVersion, introduced, destination })!;
        }

        private static Dictionary<string, DiagnosticSeverity> ReadAnalyzerReleaseSeverities()
        {
            var result = new Dictionary<string, DiagnosticSeverity>();
            foreach (string fileName in new[] { "AnalyzerReleases.Shipped.md", "AnalyzerReleases.Unshipped.md" })
            {
                string path = FindRepoFile(Path.Combine("src", "Raven.Analyzers", fileName));
                foreach (string raw in File.ReadAllLines(path))
                {
                    string line = raw.Trim();
                    if (!line.StartsWith("RVN", StringComparison.Ordinal))
                        continue;

                    string[] columns = line.Split('|');
                    if (columns.Length < 3)
                        continue;

                    string id = columns[0].Trim();
                    string severityText = columns[2].Trim();
                    if (!Enum.TryParse(severityText, out DiagnosticSeverity severity))
                        continue;

                    result[id] = severity;
                }
            }

            return result;
        }

        private static string FindRepoFile(string relativePath)
        {
            DirectoryInfo? dir = new(AppContext.BaseDirectory);
            while (dir != null)
            {
                string candidate = Path.Combine(dir.FullName, relativePath);
                if (File.Exists(candidate))
                    return candidate;

                dir = dir.Parent;
            }

            throw new FileNotFoundException($"Could not locate '{relativePath}' walking up from {AppContext.BaseDirectory}.");
        }

        private sealed class PolicyRow
        {
            public PolicyRow(string id, string introduced, DiagnosticSeverity destination, DiagnosticSeverity effective)
            {
                Id = id;
                Introduced = introduced;
                Destination = destination;
                Effective = effective;
            }

            public string Id { get; }
            public string Introduced { get; }
            public DiagnosticSeverity Destination { get; }
            public DiagnosticSeverity Effective { get; }

            public override string ToString() => $"{Id} start={Introduced} dest={Destination} effective={Effective}";
        }
    }
}
