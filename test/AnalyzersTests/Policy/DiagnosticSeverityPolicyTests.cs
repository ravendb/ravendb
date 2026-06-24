using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Shared;
using Xunit;

namespace AnalyzersTests.Policy
{
    /// <summary>
    /// Enforces the graduated-severity policy without restating it: every diagnostic declares its
    /// Start (introduced) version and destination severity once, in the analyzer's <c>Create</c>
    /// factory, and <see cref="DiagnosticDescriptors.Policies"/> is the single source of truth this
    /// test reads. A rule ships as Info in the release that introduces it and is promoted to its
    /// destination severity once the product moves past that release (see <see cref="SeverityPolicy"/>).
    /// Adding a rule needs no change here.
    /// </summary>
    public class DiagnosticSeverityPolicyTests
    {
        [Fact]
        public void Every_DiagnosticId_Has_Exactly_One_Policy_Entry()
        {
            HashSet<string> declaredIds = new(AllDiagnosticIds(), StringComparer.Ordinal);
            List<string> policyIds = DiagnosticDescriptors.Policies.Select(p => p.Id).ToList();

            // Duplicate policy entries only: compare the policy list against its own distinct set so this
            // check is about duplicates alone. Missing or stray ids are caught by the set-equality below,
            // which produces a useful diff instead of a misleading "more than one policy entry" message.
            Assert.True(policyIds.Count == policyIds.ToHashSet().Count, "Diagnostic ids with more than one policy entry exist!");

            // Set equality between the declared ids and the policy registry. A new rule that forgets to
            // go through Create (so it has no policy entry), or a stray entry, fails here.
            Assert.Equal(
                declaredIds.OrderBy(id => id, StringComparer.Ordinal),
                policyIds.OrderBy(id => id, StringComparer.Ordinal));
        }

        [Fact]
        public void Effective_Severity_Follows_The_Version_Policy()
        {
            // EffectiveSeverity is the descriptor's shipped DefaultSeverity; it must equal what the
            // version resolver derives from the declared Start version and destination severity.
            foreach (DiagnosticSeverityPolicyEntry policy in DiagnosticDescriptors.Policies)
            {
                DiagnosticSeverity expected = SeverityPolicy.Resolve(policy.IntroducedAt, policy.DestinationSeverity);
                Assert.Equal(expected, policy.EffectiveSeverity);
            }
        }

        [Fact]
        public void AnalyzerReleases_Severities_Are_In_Sync_With_Code()
        {
            Dictionary<string, DiagnosticSeverity> tracked = ReadAnalyzerReleaseSeverities();
            Dictionary<string, DiagnosticSeverityPolicyEntry> byId =
                DiagnosticDescriptors.Policies.ToDictionary(p => p.Id, StringComparer.Ordinal);

            foreach (DiagnosticSeverityPolicyEntry policy in byId.Values)
            {
                Assert.True(tracked.TryGetValue(policy.Id, out DiagnosticSeverity severity),
                    $"{policy.Id} is not listed in any AnalyzerReleases file.");
                Assert.Equal(policy.EffectiveSeverity, severity);
            }

            string[] strayTracked = tracked.Keys.Where(id => !byId.ContainsKey(id)).ToArray();
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
            Assert.Equal(expected, SeverityPolicy.Resolve(productVersion, introduced, destination));
        }

        private static string[] AllDiagnosticIds() =>
            typeof(DiagnosticIds)
                .GetFields(BindingFlags.Public | BindingFlags.Static)
                .Where(f => f.IsLiteral && f.FieldType == typeof(string))
                .Select(f => (string)f.GetRawConstantValue()!)
                .ToArray();

        private static Dictionary<string, DiagnosticSeverity> ReadAnalyzerReleaseSeverities()
        {
            var result = new Dictionary<string, DiagnosticSeverity>(StringComparer.Ordinal);
            foreach (string fileName in new[] { "AnalyzerReleases.Shipped.md", "AnalyzerReleases.Unshipped.md" })
            {
                string path = FindRepoFile(Path.Combine("src", "Raven.Analyzers", fileName));

                // Duplicates are tracked per file: the same rule may legitimately appear in both
                // Shipped (its original release) and Unshipped (under Changed Rules), in which case
                // the Unshipped value must win. But two rows for the same id inside one file is a
                // copy-paste mistake, so fail fast instead of silently overwriting.
                var seenInFile = new HashSet<string>(StringComparer.Ordinal);

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

                    if (seenInFile.Add(id) == false)
                        throw new InvalidOperationException($"Duplicate rule id '{id}' found in {fileName}.");

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
    }
}
