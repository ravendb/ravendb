using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Generators;

namespace AnalyzersTests.Framework
{
    /// <summary>
    /// How a referenced project reaches the compilation under analysis.
    /// </summary>
    /// <remarks>
    /// The two hosts differ and the difference is load-bearing, so tests cover both. A command-line
    /// build compiles each project separately and passes a compiled DLL, whose symbols carry no syntax.
    /// An IDE passes a <see cref="CompilationReference"/>, whose symbols do carry syntax, but it belongs
    /// to the other compilation, so asking this one for a semantic model over it throws. An analyzer that
    /// reads index shape from recorded metadata is unaffected either way, which is the property the
    /// cross-project tests pin down.
    /// </remarks>
    public enum ReferenceKind
    {
        CompiledDll,
        CompilationReference
    }

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
        /// The same reference set the analyzer tests compile against, for tests that need to drive a
        /// compilation themselves rather than go through <see cref="AnalyzeAsync{TAnalyzer}"/>.
        /// </summary>
        internal static IReadOnlyList<MetadataReference> MetadataReferences => DefaultReferences.Value;

        /// <summary>
        /// Parses <paramref name="source"/>, compiles it with Raven.Client referenced,
        /// runs <typeparamref name="TAnalyzer"/>, and returns diagnostics ordered by source position.
        /// Throws <see cref="InvalidOperationException"/> if the compilation has any errors,
        /// so that test failures aren't silently masked by broken reference resolution.
        /// </summary>
        internal static async Task<ImmutableArray<Diagnostic>> AnalyzeAsync<TAnalyzer>(string source)
            where TAnalyzer : DiagnosticAnalyzer, new()
        {
            // Match the project's settings (LangVersion=preview, Nullable=enable) so snippets compile
            // and behave the same way the analyzers see them at build time.
            CSharpParseOptions parseOptions = new(LanguageVersion.Preview);
            SyntaxTree tree = CSharpSyntaxTree.ParseText(source, parseOptions);

            CSharpCompilation compilation = CSharpCompilation.Create(
                assemblyName: "TestAssembly",
                syntaxTrees: [tree],
                references: DefaultReferences.Value,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                    .WithNullableContextOptions(NullableContextOptions.Enable));

            // Run the index-metadata generator first, exactly as csc does: the analyzers read an index's
            // shape from the RavenIndexMetadataAttribute its assembly records rather than from the index
            // constructor, so without this step no test snippet would have any index metadata to read.
            // The driver gets the same parse options as the trees above; mismatched ones make
            // AddSyntaxTrees throw "Inconsistent language versions".
            CSharpGeneratorDriver.Create(
                    new[] { new IndexMetadataGenerator().AsSourceGenerator() },
                    parseOptions: parseOptions)
                .RunGeneratorsAndUpdateCompilation(compilation, out Compilation generated, out ImmutableArray<Diagnostic> generatorDiagnostics);

            ImmutableArray<Diagnostic> generatorErrors = generatorDiagnostics
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .ToImmutableArray();

            if (!generatorErrors.IsEmpty)
            {
                throw new InvalidOperationException(
                    $"Index metadata generator reported {generatorErrors.Length} error(s):\n"
                    + string.Join("\n", generatorErrors.Select(d => d.ToString())));
            }

            compilation = (CSharpCompilation)generated;

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

        /// <summary>
        /// Compiles <paramref name="referencedSource"/> as a separate project, references it from a
        /// compilation of <paramref name="source"/> the way <paramref name="referenceKind"/> specifies,
        /// and runs <typeparamref name="TAnalyzer"/> over the latter. The metadata generator runs on
        /// both, mirroring a real build where every project runs it.
        /// </summary>
        internal static async Task<ImmutableArray<Diagnostic>> AnalyzeWithReferencedProjectAsync<TAnalyzer>(
            string referencedSource,
            string source,
            ReferenceKind referenceKind)
            where TAnalyzer : DiagnosticAnalyzer, new()
        {
            CSharpParseOptions parseOptions = new(LanguageVersion.Preview);

            CSharpCompilation referenced = Compile("ReferencedAssembly", referencedSource, DefaultReferences.Value, parseOptions);
            referenced = RunMetadataGenerator(referenced, parseOptions, "referenced project");
            AssertNoCompileErrors(referenced, "Referenced project");

            MetadataReference reference = referenceKind switch
            {
                ReferenceKind.CompilationReference => referenced.ToMetadataReference(),
                _ => ToCompiledDll(referenced)
            };

            CSharpCompilation compilation = Compile(
                "TestAssembly", source, [.. DefaultReferences.Value, reference], parseOptions);
            compilation = RunMetadataGenerator(compilation, parseOptions, "test project");
            AssertNoCompileErrors(compilation, "Test");

            TAnalyzer analyzer = new();
            ImmutableArray<Diagnostic> diagnostics = await compilation
                .WithAnalyzers(ImmutableArray.Create<DiagnosticAnalyzer>(analyzer))
                .GetAllDiagnosticsAsync();

            // AD0001 means the analyzer threw. Surface it as a failure rather than letting it read as
            // "no diagnostics reported", which is how the cross-project crash originally hid.
            ImmutableArray<Diagnostic> crashes = diagnostics
                .Where(d => d.Id == "AD0001")
                .ToImmutableArray();

            if (!crashes.IsEmpty)
            {
                throw new InvalidOperationException(
                    $"Analyzer threw {crashes.Length} exception(s):\n"
                    + string.Join("\n", crashes.Select(d => d.GetMessage())));
            }

            return [.. diagnostics.Where(d => d.Id.StartsWith("RVN", StringComparison.Ordinal))
                                  .OrderBy(d => d.Location.SourceSpan.Start)];
        }

        private static CSharpCompilation Compile(
            string assemblyName,
            string source,
            IEnumerable<MetadataReference> references,
            CSharpParseOptions parseOptions) =>
            CSharpCompilation.Create(
                assemblyName,
                [CSharpSyntaxTree.ParseText(source, parseOptions)],
                references,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                    .WithNullableContextOptions(NullableContextOptions.Enable));

        private static CSharpCompilation RunMetadataGenerator(
            CSharpCompilation compilation,
            CSharpParseOptions parseOptions,
            string label)
        {
            CSharpGeneratorDriver.Create(
                    new[] { new IndexMetadataGenerator().AsSourceGenerator() },
                    parseOptions: parseOptions)
                .RunGeneratorsAndUpdateCompilation(compilation, out Compilation generated, out ImmutableArray<Diagnostic> generatorDiagnostics);

            ImmutableArray<Diagnostic> errors = generatorDiagnostics
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .ToImmutableArray();

            if (!errors.IsEmpty)
            {
                throw new InvalidOperationException(
                    $"Index metadata generator reported {errors.Length} error(s) for the {label}:\n"
                    + string.Join("\n", errors.Select(d => d.ToString())));
            }

            return (CSharpCompilation)generated;
        }

        private static MetadataReference ToCompiledDll(CSharpCompilation compilation)
        {
            var peStream = new MemoryStream();
            Microsoft.CodeAnalysis.Emit.EmitResult result = compilation.Emit(peStream);

            if (!result.Success)
            {
                throw new InvalidOperationException(
                    "Referenced project failed to emit:\n"
                    + string.Join("\n", result.Diagnostics
                        .Where(d => d.Severity == DiagnosticSeverity.Error)
                        .Select(d => d.ToString())));
            }

            peStream.Position = 0;
            return MetadataReference.CreateFromStream(peStream);
        }

        private static void AssertNoCompileErrors(CSharpCompilation compilation, string label)
        {
            ImmutableArray<Diagnostic> compileErrors = compilation.GetDiagnostics()
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .ToImmutableArray();

            if (!compileErrors.IsEmpty)
            {
                throw new InvalidOperationException(
                    $"{label} compilation has {compileErrors.Length} error(s):\n"
                    + string.Join("\n", compileErrors.Select(d => d.ToString())));
            }
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
