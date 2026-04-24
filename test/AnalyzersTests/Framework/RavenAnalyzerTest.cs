using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;

namespace AnalyzersTests.Framework
{
    /// <summary>
    /// Lightweight Roslyn analyzer test harness.
    /// Creates an in-memory C# compilation that includes Raven.Client as a reference,
    /// runs the specified analyzer against it, and returns the reported diagnostics.
    /// </summary>
    internal static class RavenAnalyzerTest
    {
        private static readonly Lazy<IReadOnlyList<MetadataReference>> DefaultReferences =
            new(BuildDefaultReferences);

        /// <summary>
        /// Parses <paramref name="source"/>, compiles it with Raven.Client referenced,
        /// runs <typeparamref name="TAnalyzer"/>, and returns diagnostics ordered by source position.
        /// Throws <see cref="InvalidOperationException"/> if the compilation has any errors,
        /// so that test failures aren't silently masked by broken reference resolution.
        /// </summary>
        internal static async Task<ImmutableArray<Diagnostic>> AnalyzeAsync<TAnalyzer>(string source)
            where TAnalyzer : DiagnosticAnalyzer, new()
        {
            SyntaxTree tree = CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Latest));

            CSharpCompilation compilation = CSharpCompilation.Create(
                assemblyName: "TestAssembly",
                syntaxTrees: [tree],
                references: DefaultReferences.Value,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

            // Fail fast if the compilation has errors — analyzer diagnostics are meaningless otherwise
            ImmutableArray<Diagnostic> compileErrors = compilation.GetDiagnostics()
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .ToImmutableArray();

            if (!compileErrors.IsEmpty)
            {
                string errors = string.Join("\n", compileErrors.Select(d => d.ToString()));
                throw new InvalidOperationException(
                    $"Test compilation has {compileErrors.Length} error(s):\n{errors}");
            }

            TAnalyzer analyzer = new();
            CompilationWithAnalyzers compilationWithAnalyzers =
                compilation.WithAnalyzers(ImmutableArray.Create<DiagnosticAnalyzer>(analyzer));

            ImmutableArray<Diagnostic> diagnostics =
                await compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync();

            return [.. diagnostics.OrderBy(d => d.Location.SourceSpan.Start)];
        }

        private static IReadOnlyList<MetadataReference> BuildDefaultReferences()
        {
            HashSet<string> added = new(StringComparer.OrdinalIgnoreCase);
            List<MetadataReference> refs = [];

            // BCL and runtime assemblies — TRUSTED_PLATFORM_ASSEMBLIES has absolute paths
            string trusted = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") as string ?? string.Empty;
            foreach (string path in trusted.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
                TryAdd(path);

            // Raven.Client and its co-located dependencies (Sparrow, etc.) from the test output dir
            string outputDir = Path.GetDirectoryName(
                typeof(Raven.Client.Documents.Session.IDocumentSession).Assembly.Location)!;
            foreach (string dll in Directory.GetFiles(outputDir, "*.dll"))
                TryAdd(dll);

            return refs;

            void TryAdd(string path)
            {
                if (!added.Add(path)) return;
                try
                {
                    // Skip native DLLs — PEReader throws if there is no managed metadata
                    using var stream = File.OpenRead(path);
                    using var pe = new System.Reflection.PortableExecutable.PEReader(stream);
                    if (!pe.HasMetadata) return;
                    refs.Add(MetadataReference.CreateFromFile(path));
                }
                catch { /* skip unreadable or non-managed files */ }
            }
        }
    }
}
