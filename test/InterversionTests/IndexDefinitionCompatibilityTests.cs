using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using InterversionTests.IndexDefinitionCompatibility;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class IndexDefinitionCompatibilityTests : InterversionTestBase
    {
        private const string BaselineClientVersion = "6.2.18";
        private const string BaselineServerVersion = "6.2.18";
        private const int ExpectedDefinitionCount = 312;
        private const int ServerRoundTripBatchSize = 16;

        private readonly ITestOutputHelper _output;

        // These definitions remain part of the released/current client-text comparison,
        // but are excluded from the symmetric server round-trip because 6.2.18 rejects them.
        // This is not the list of behaviors fixed by the candidate server.
        private static readonly HashSet<string> DefinitionsExcludedFromServerRoundTrip = new(StringComparer.Ordinal)
        {
            "operation/legacy-matrix/bool/aggregate",
            "operation/legacy-matrix/char/where-select",
            "operation/legacy-matrix/double/take-skip",
            "operation/legacy-matrix/double/where-select",
            "operation/legacy-matrix/float/take-skip",
            "operation/legacy-matrix/float/where-select",
            "operation/legacy-matrix/long/take-skip",
            "operation/legacy-matrix/long/where-select",
            "topology/binary-precedence/boolean-and",
            "topology/binary-precedence/boolean-or"
        };

        public IndexDefinitionCompatibilityTests(ITestOutputHelper output) : base(output)
        {
            _output = output;
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Indexes, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task StaticIndexDefinitionTextRemainsIdenticalAcrossReleasedAndCurrentClientsAndServers()
        {
            var candidate = DefinitionGeneratorProgram.Generate();
            var cachedGenerator = await ReleasedClientGeneratorCache.GetAsync(BaselineClientVersion, candidate.CompilerFingerprint);

            _output.WriteLine(
                $"{(cachedGenerator.WasBuilt ? "Built" : "Reused")} released RavenDB.Client {BaselineClientVersion} generator: {cachedGenerator.CacheDirectory}");

            var baseline = await RunGenerator(cachedGenerator.AssemblyPath);

            Assert.Equal(ExpectedDefinitionCount, baseline.Definitions.Count);
            Assert.Equal(baseline.CompilerFingerprint, candidate.CompilerFingerprint);
            Assert.StartsWith(BaselineServerVersion, baseline.RavenClientProductVersion, StringComparison.Ordinal);
            AssertDefinitionSetsEqual("The released and current clients generated different definitions.", baseline.Definitions, candidate.Definitions);

            var unknownExclusions = DefinitionsExcludedFromServerRoundTrip
                .Where(x => baseline.Definitions.ContainsKey(x) == false)
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToArray();
            Assert.True(unknownExclusions.Length == 0, "Unknown server round-trip exclusions: " + string.Join(", ", unknownExclusions));

            using (DocumentStore baselineStore = await GetDocumentStoreAsync(BaselineServerVersion))
            using (DocumentStore candidateStore = GetDocumentStore())
            {
                await AssertDefinitionsRoundTrip(baselineStore, candidateStore, baseline.Definitions, candidate.Definitions);
            }
        }

        private const string CurrentServer = "current";
        private const string PinnedIndexingEngine = "Lucene";

        [RavenMultiplatformTheory(RavenTestCategory.Interversion | RavenTestCategory.Indexes, RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData(BaselineServerVersion, "Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int>", 1)]
        [InlineData(CurrentServer, "Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int>", 1)]
        [InlineData(BaselineServerVersion, "Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int> { }", 1)]
        [InlineData(CurrentServer, "Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int> { }", 1)]
        [InlineData(BaselineServerVersion, "Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int> or IEnumerable<long>", 1)]
        [InlineData(CurrentServer, "Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int> or IEnumerable<long>", 1)]
        [InlineData(BaselineServerVersion, "Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int> and not null", 1)]
        [InlineData(CurrentServer, "Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<int> and not null", 1)]
        [InlineData(BaselineServerVersion, "(Enumerable.Distinct((dynamic)new[] { 1, 2 }) switch { IEnumerable<int> values => values.Sum() == 3, _ => false })", 1)]
        [InlineData(CurrentServer, "(Enumerable.Distinct((dynamic)new[] { 1, 2 }) switch { IEnumerable<int> values => values.Sum() == 3, _ => false })", 1)]
        [InlineData(BaselineServerVersion, "Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<object> or IEnumerable<long>", 0)]
        [InlineData(CurrentServer, "Enumerable.Distinct((dynamic)new[] { 1, 2 }) is IEnumerable<object> or IEnumerable<long>", 0)]
        public async Task NestedDistinctInsideCountPredicateStoresOneOnReleasedAndCurrentServers(string server, string predicate, int expected)
        {
            const string indexName = "DynamicEnumerableCompatibility/NestedDistinctElementType";
            string map = "from item in docs.Items select new { N = Enumerable.Count(item.Tags, tag => " + predicate + ") }";
            using DocumentStore store = await OpenStoreWithPinnedEngineAsync(server);
            await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { map },
                Fields = { ["N"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            Assert.Equal(map, Assert.Single((await store.Maintenance.SendAsync(new GetIndexOperation(indexName))).Maps));
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item { Tags = new[] { "only" } }, "items/1");
                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, allowErrors: true);
            IndexStats stats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(indexName));
            IndexingError[] errors = (await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { indexName }))).Single().Errors;
            List<StoredCountRow> rows;
            using (var session = store.OpenSession())
                rows = session.Advanced.RawQuery<StoredCountRow>($"from index '{indexName}' select N").ToList();

            string observed = $"[{server}] predicate={predicate} expected={expected} map={map} document=items/1 {{ Tags = [\"only\"] }} MapAttempts={stats.MapAttempts} MapErrors={stats.MapErrors} EntriesCount={stats.EntriesCount} " +
                              $"errors=[{string.Join(" | ", errors.Select(x => x.Error))}] rows={rows.Count} N=[{string.Join(", ", rows.Select(x => x.N))}]";
            _output.WriteLine(observed);
            Assert.True(errors.Length == 0, "Expected no index errors. " + observed);
            Assert.True(rows.Count == 1, "Expected exactly one stored row. " + observed);
            Assert.True(rows[0].N == expected, $"Expected N == {expected}. " + observed);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion | RavenTestCategory.Indexes, RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData(BaselineServerVersion, "Enumerable.Contains(d.Age, 5)")]
        [InlineData(CurrentServer, "Enumerable.Contains(d.Age, 5)")]
        [InlineData(BaselineServerVersion, "Enumerable.SequenceEqual(d.Tags, d.Age)")]
        [InlineData(CurrentServer, "Enumerable.SequenceEqual(d.Tags, d.Age)")]
        public async Task ScalarSequenceOperandFailsIndexingOnReleasedAndCurrentServers(string server, string expression)
        {
            const string indexName = "DynamicEnumerableCompatibility/ScalarSequenceOperand";
            string map = "from d in docs select new { Has = " + expression + " }";
            using DocumentStore store = await OpenStoreWithPinnedEngineAsync(server);
            await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { map },
                Fields = { ["Has"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));
            Assert.Equal(map, Assert.Single((await store.Maintenance.SendAsync(new GetIndexOperation(indexName))).Maps));
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Person { Age = 5L, Tags = new[] { "a" } }, "people/1");
                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, allowErrors: true);
            IndexStats stats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(indexName));
            IndexingError[] errors = (await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { indexName }))).Single().Errors;

            string observed = $"[{server}] map={map} document=people/1 {{ Age = 5L, Tags = [\"a\"] }} MapAttempts={stats.MapAttempts} MapErrors={stats.MapErrors} EntriesCount={stats.EntriesCount} IsStale={stats.IsStale} State={stats.State} " +
                              $"errors=[{string.Join(" | ", errors.Select(x => $"{x.Action} {x.Document}: {x.Error}"))}]";
            _output.WriteLine(observed);
            Assert.True(stats.MapErrors == 1, "Expected MapErrors == 1. " + observed);
            Assert.True(stats.EntriesCount == 0, "Expected EntriesCount == 0. " + observed);
            IndexingError error = Assert.Single(errors);
            Assert.Equal("people/1", error.Document);
            Assert.Contains("RuntimeBinderException", error.Error);
        }

        private async Task<DocumentStore> OpenStoreWithPinnedEngineAsync(string server, [CallerMemberName] string caller = null)
        {
            string engineKey = RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType);
            DocumentStore store = server == CurrentServer
                ? GetDocumentStore(new Options { ModifyDatabaseRecord = record => record.Settings[engineKey] = PinnedIndexingEngine }, caller: caller)
                : await GetDocumentStoreAsync(server, new InterversionTestOptions { ModifyDatabaseRecord = record => record.Settings[engineKey] = PinnedIndexingEngine }, database: caller);

            BuildNumber build = await store.Maintenance.Server.SendAsync(new GetBuildNumberOperation());
            _output.WriteLine($"[{server}] server at {store.Urls[0]}: FullVersion={build.FullVersion} ProductVersion={build.ProductVersion} BuildVersion={build.BuildVersion} CommitHash={build.CommitHash} engine={PinnedIndexingEngine}");
            if (server == CurrentServer)
                Assert.False(build.FullVersion.StartsWith(BaselineServerVersion, StringComparison.Ordinal), $"The current server reported the baseline version {build.FullVersion}.");
            else
                Assert.StartsWith(server, build.FullVersion, StringComparison.Ordinal);

            return store;
        }

        private sealed class Item
        {
            public string[] Tags { get; set; }
        }

        private sealed class Person
        {
            public long Age { get; set; }

            public string[] Tags { get; set; }
        }

        private sealed class StoredCountRow
        {
            public int N { get; set; }
        }

        private static async Task<GeneratorOutput> RunGenerator(string generatorPath)
        {
            Assert.True(File.Exists(generatorPath), $"Released client generator assembly was not found at '{generatorPath}'.");

            var startInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            startInfo.ArgumentList.Add(generatorPath);

            using var process = Process.Start(startInfo);
            Assert.NotNull(process);

            var standardOutput = process.StandardOutput.ReadToEndAsync();
            var standardError = process.StandardError.ReadToEndAsync();
            await process.WaitForExitAsync();

            var output = await standardOutput;
            var error = await standardError;
            Assert.True(process.ExitCode == 0, $"Released client generator exited with {process.ExitCode}.\nSTDOUT:\n{output}\nSTDERR:\n{error}");

            var result = JsonSerializer.Deserialize<GeneratorOutput>(output);
            Assert.NotNull(result);
            Assert.NotNull(result.Definitions);
            return result;
        }

        private static void AssertDefinitionSetsEqual(string context, IReadOnlyDictionary<string, DefinitionText> expected, IReadOnlyDictionary<string, DefinitionText> actual)
        {
            var expectedCaseIds = expected.Keys.OrderBy(x => x, StringComparer.Ordinal).ToArray();
            var actualCaseIds = actual.Keys.OrderBy(x => x, StringComparer.Ordinal).ToArray();
            Assert.True(expectedCaseIds.SequenceEqual(actualCaseIds, StringComparer.Ordinal),
                $"{context}{Environment.NewLine}" +
                $"Expected CaseIds: {string.Join(", ", expectedCaseIds)}{Environment.NewLine}" +
                $"Actual CaseIds: {string.Join(", ", actualCaseIds)}");

            foreach (string caseId in expectedCaseIds)
                AssertDefinitionTextEqual(context, caseId, expected[caseId], actual[caseId]);
        }

        private static async Task AssertDefinitionsRoundTrip(
            IDocumentStore baselineStore,
            IDocumentStore candidateStore,
            IReadOnlyDictionary<string, DefinitionText> baseline,
            IReadOnlyDictionary<string, DefinitionText> candidate)
        {
            var caseIds = baseline.Keys
                .Where(x => DefinitionsExcludedFromServerRoundTrip.Contains(x) == false)
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToArray();
            Assert.Equal(ExpectedDefinitionCount - DefinitionsExcludedFromServerRoundTrip.Count, caseIds.Length);

            for (int offset = 0; offset < caseIds.Length; offset += ServerRoundTripBatchSize)
            {
                var batchCaseIds = caseIds.Skip(offset).Take(ServerRoundTripBatchSize).ToArray();
                var baselineDefinitions = CreateIndexDefinitions(batchCaseIds, baseline);
                var candidateDefinitions = CreateIndexDefinitions(batchCaseIds, candidate);

                await baselineStore.Maintenance.SendAsync(new PutIndexesOperation(baselineDefinitions));
                await candidateStore.Maintenance.SendAsync(new PutIndexesOperation(candidateDefinitions));

                var baselineByName = await ReadSubmittedDefinitionsAfterSideBySideReplacement(baselineStore, baselineDefinitions);
                var candidateByName = await ReadSubmittedDefinitionsAfterSideBySideReplacement(candidateStore, candidateDefinitions);

                for (int i = 0; i < batchCaseIds.Length; i++)
                {
                    var caseId = batchCaseIds[i];
                    var indexName = CreateIndexName(i);
                    Assert.True(baselineByName.TryGetValue(indexName, out IndexDefinition baselineDefinition), $"The 6.2.18 server did not return '{indexName}' for '{caseId}'.");
                    Assert.True(candidateByName.TryGetValue(indexName, out IndexDefinition candidateDefinition), $"The current server did not return '{indexName}' for '{caseId}'.");

                    AssertDefinitionTextEqual("The 6.2.18 server changed the submitted definition.",
                        caseId, expected: baseline[caseId], actual: ToDefinitionText(baselineDefinition));

                    AssertDefinitionTextEqual("The current server changed the submitted definition.",
                        caseId, expected: candidate[caseId], actual: ToDefinitionText(candidateDefinition));
                }
            }
        }

        private static async Task<Dictionary<string, IndexDefinition>> ReadSubmittedDefinitionsAfterSideBySideReplacement(IDocumentStore store, IReadOnlyList<IndexDefinition> submittedDefinitions)
        {
            Dictionary<string, IndexDefinition> storedByName;
            var submittedByName = submittedDefinitions.ToDictionary(
                x => x.Name,
                ToDefinitionText,
                StringComparer.Ordinal);
            var timeout = Stopwatch.StartNew();

            do
            {
                var stored = await store.Maintenance.SendAsync(new GetIndexesOperation(0, ServerRoundTripBatchSize));
                storedByName = stored.ToDictionary(x => x.Name, StringComparer.Ordinal);

                var allSubmittedDefinitionsAreActive = submittedByName.All(x =>
                    storedByName.TryGetValue(x.Key, out IndexDefinition storedDefinition) &&
                    DefinitionTextEquals(x.Value, ToDefinitionText(storedDefinition)));

                if (allSubmittedDefinitionsAreActive)
                    return storedByName;

                await Task.Delay(100);
            } while (timeout.Elapsed < TimeSpan.FromSeconds(15));

            return storedByName;
        }

        private static IndexDefinition[] CreateIndexDefinitions(IReadOnlyList<string> caseIds, IReadOnlyDictionary<string, DefinitionText> definitions)
        {
            var result = new IndexDefinition[caseIds.Count];
            for (int i = 0; i < caseIds.Count; i++)
            {
                var definition = definitions[caseIds[i]];
                result[i] = new IndexDefinition
                {
                    Name = CreateIndexName(i),
                    Maps = definition.Maps.ToHashSet(StringComparer.Ordinal),
                    Reduce = definition.Reduce
                };
            }
            return result;
        }

        private static DefinitionText ToDefinitionText(IndexDefinition definition)
        {
            return new DefinitionText
            {
                Maps = definition.Maps.OrderBy(x => x, StringComparer.Ordinal).ToArray(),
                Reduce = definition.Reduce
            };
        }

        private static bool DefinitionTextEquals(DefinitionText expected, DefinitionText actual)
        {
            return expected.Maps.SequenceEqual(actual.Maps, StringComparer.Ordinal) &&
                   string.Equals(expected.Reduce, actual.Reduce, StringComparison.Ordinal);
        }

        private static void AssertDefinitionTextEqual(string context, string caseId, DefinitionText expected, DefinitionText actual)
        {
            Assert.True(DefinitionTextEquals(expected, actual), BuildDefinitionMismatchReport(context, caseId, expected, actual));
        }

        private static string BuildDefinitionMismatchReport(string context, string caseId, DefinitionText expected, DefinitionText actual)
        {
            var report = new StringBuilder();
            report.AppendLine(context);
            report.AppendLine($"CaseId: {caseId}");
            report.AppendLine($"First difference: {FindFirstDifference(expected, actual)}");
            AppendDefinition(report, "Expected", expected);
            AppendDefinition(report, "Actual", actual);
            return report.ToString();
        }

        private static void AppendDefinition(StringBuilder report, string name, DefinitionText definition)
        {
            for (int i = 0; i < definition.Maps.Length; i++)
            {
                report.AppendLine($"{name} map[{i}] ({definition.Maps[i].Length} chars):");
                report.AppendLine(definition.Maps[i]);
            }

            report.AppendLine($"{name} reduce ({(definition.Reduce == null ? "null" : definition.Reduce.Length + " chars")}):");
            report.AppendLine(definition.Reduce ?? "<null>");
        }

        private static string FindFirstDifference(DefinitionText expected, DefinitionText actual)
        {
            var commonMapCount = Math.Min(expected.Maps.Length, actual.Maps.Length);
            for (int i = 0; i < commonMapCount; i++)
            {
                var difference = FindFirstTextDifference(expected.Maps[i], actual.Maps[i]);
                if (difference != null)
                    return $"map[{i}] {difference}";
            }

            if (expected.Maps.Length != actual.Maps.Length)
                return $"map count {expected.Maps.Length} != {actual.Maps.Length}";

            return "reduce " + FindFirstTextDifference(expected.Reduce, actual.Reduce);
        }

        private static string FindFirstTextDifference(string expected, string actual)
        {
            if (string.Equals(expected, actual, StringComparison.Ordinal))
                return null;

            if (expected == null || actual == null)
                return $"nullability differs: expected={(expected == null ? "null" : "value")}, actual={(actual == null ? "null" : "value")}";

            var commonLength = Math.Min(expected.Length, actual.Length);
            var index = 0;
            while (index < commonLength && expected[index] == actual[index])
                index++;

            return index == commonLength
                ? $"length differs at char {index}: expected={expected.Length}, actual={actual.Length}"
                : $"at char {index}: expected U+{(int)expected[index]:X4}, actual U+{(int)actual[index]:X4}";
        }

        private static string CreateIndexName(int ordinal)
        {
            return $"IndexDefinitionCompatibility/{ordinal:D3}";
        }
    }
}
