using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Text;

namespace AnalyzersTests.Framework
{
    /// <summary>
    /// Test harness for code fixes. Compiles source using the same reference setup as RavenAnalyzerTest,
    /// runs the specified analyzer and code fix provider, and returns the fixed source code.
    /// </summary>
    internal static class RavenCodeFixTest
    {
        private static readonly Lazy<IReadOnlyList<MetadataReference>> DefaultReferences =
            new(BuildDefaultReferences);

        /// <summary>
        /// Applies the first code fix offered by <typeparamref name="TFix"/> for the first
        /// diagnostic produced by <typeparamref name="TAnalyzer"/> in <paramref name="source"/>.
        /// Returns the resulting document text. Throws if no diagnostics or fixes are found.
        /// </summary>
        internal static async Task<string> ApplyFixAsync<TAnalyzer, TFix>(string source)
            where TAnalyzer : DiagnosticAnalyzer, new()
            where TFix : CodeFixProvider, new()
        {
            SyntaxTree tree = CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Latest));

            CSharpCompilation compilation = CSharpCompilation.Create(
                assemblyName: "TestAssembly",
                syntaxTrees: [tree],
                references: DefaultReferences.Value,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

            // Fail fast if the compilation has errors
            ImmutableArray<Diagnostic> compileErrors = compilation.GetDiagnostics()
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .ToImmutableArray();

            if (!compileErrors.IsEmpty)
            {
                string errors = string.Join("\n", compileErrors.Select(d => d.ToString()));
                throw new InvalidOperationException(
                    $"Test compilation has {compileErrors.Length} error(s):\n{errors}");
            }

            // Run the analyzer to get diagnostics
            TAnalyzer analyzer = new();
            CompilationWithAnalyzers compilationWithAnalyzers =
                compilation.WithAnalyzers(ImmutableArray.Create<DiagnosticAnalyzer>(analyzer));

            ImmutableArray<Diagnostic> diagnostics = await compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync();

            if (diagnostics.IsEmpty)
                throw new InvalidOperationException("Expected at least one diagnostic from the analyzer.");

            // Create workspace and document
            using AdhocWorkspace workspace = new();
            ProjectInfo projectInfo = ProjectInfo.Create(
                ProjectId.CreateNewId(),
                VersionStamp.Create(),
                name: "TestProject",
                assemblyName: "TestAssembly",
                language: LanguageNames.CSharp)
                .WithMetadataReferences(DefaultReferences.Value);

            Project project = workspace.AddProject(projectInfo);

            DocumentInfo docInfo = DocumentInfo.Create(
                DocumentId.CreateNewId(project.Id),
                name: "TestDocument.cs",
                sourceCodeKind: SourceCodeKind.Regular,
                loader: TextLoader.From(TextAndVersion.Create(SourceText.From(source), VersionStamp.Create())));

            Document document = workspace.AddDocument(docInfo);

            // Get the root and semantic model for the document
            SyntaxNode? root = await document.GetSyntaxRootAsync();
            SemanticModel? semanticModel = await document.GetSemanticModelAsync();

            if (root == null || semanticModel == null)
                throw new InvalidOperationException("Failed to get syntax root or semantic model.");

            // Use the first diagnostic
            Diagnostic firstDiagnostic = diagnostics[0];

            // Create a code fix context
            CodeFixContext context = new(
                document: document,
                diagnostic: firstDiagnostic,
                registerCodeFix: (action, diags) => { /* collected in the inner callback */ },
                cancellationToken: CancellationToken.None);

            TFix fixProvider = new();
            List<CodeAction> registeredActions = [];

            // Re-run RegisterCodeFixesAsync with a callback that collects actions
            await fixProvider.RegisterCodeFixesAsync(
                new CodeFixContext(
                    document,
                    firstDiagnostic,
                    (action, _) => registeredActions.Add(action),
                    CancellationToken.None));

            if (registeredActions.Count == 0)
                throw new InvalidOperationException("No code fixes were registered for the first diagnostic.");

            // Apply the first registered code fix
            CodeAction firstAction = registeredActions[0];
            ImmutableArray<CodeActionOperation> operations = await firstAction.GetOperationsAsync(CancellationToken.None);

            if (operations.Length == 0)
                throw new InvalidOperationException("No operations returned from the code action.");

            // Apply all operations to the workspace
            foreach (CodeActionOperation op in operations)
            {
                if (op is ApplyChangesOperation applyOp)
                    applyOp.Apply(workspace, CancellationToken.None);
            }

            // Get the modified document
            Document? modifiedDoc = workspace.CurrentSolution.GetDocument(document.Id);
            if (modifiedDoc == null)
                throw new InvalidOperationException("Failed to retrieve modified document.");

            SourceText? text = await modifiedDoc.GetTextAsync();
            if (text == null)
                throw new InvalidOperationException("Failed to get text from modified document.");

            return text.ToString();
        }

        private static IReadOnlyList<MetadataReference> BuildDefaultReferences()
        {
            HashSet<string> added = new(StringComparer.OrdinalIgnoreCase);
            List<MetadataReference> refs = [];

            // BCL and runtime assemblies
            string trusted = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") as string ?? string.Empty;
            foreach (string path in trusted.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
                TryAdd(path);

            // Raven.Client and co-located dependencies
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
