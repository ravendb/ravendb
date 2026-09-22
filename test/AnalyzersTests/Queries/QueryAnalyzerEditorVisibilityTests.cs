using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Queries;
using Tests.Infrastructure;
using Xunit;

namespace AnalyzersTests.Queries
{
    /// <summary>
    /// Pins that the query rules are visible in an editor, not only in a full build.
    /// </summary>
    /// <remarks>
    /// The rest of the suite collects diagnostics through <c>GetAnalyzerDiagnosticsAsync</c>, which runs
    /// every action an analyzer registers, compilation-end included. An IDE does not: while you type it
    /// runs the per-file actions and skips compilation-end entirely, so a rule that reports from
    /// <c>RegisterCompilationEndAction</c> passes every test here and still shows nothing in the editor.
    /// Both query rules used to report that way, because the index-name lookup they share was filled by
    /// a symbol action that is only complete at the end of the compilation.
    /// <para>
    /// These tests ask for semantic diagnostics for a single file, which is the API an editor uses, so a
    /// return to compilation-end reporting fails them.
    /// </para>
    /// </remarks>
    public class QueryAnalyzerEditorVisibilityTests
    {
        private const string Source = """
            using System.Linq;
            using Raven.Client.Documents;
            using Raven.Client.Documents.Indexes;
            using Raven.Client.Documents.Linq;
            using Raven.Client.Documents.Queries;
            using Raven.Client.Documents.Session;

            public class Book
            {
                public string Id { get; set; }
                public string Title { get; set; }
                public string AuthorId { get; set; }
            }

            public class BookDto { public string Title { get; set; } }

            public class BooksByAuthor : AbstractIndexCreationTask<Book>
            {
                public BooksByAuthor()
                {
                    Map = books => from book in books select new { book.AuthorId };
                }
            }

            public static class Queries
            {
                public static void Filter(IDocumentSession session)
                {
                    var books = session.Query<Book, BooksByAuthor>()
                        .Where(book => book.Title == "test")
                        .Take(4)
                        .ToArray();
                }

                public static void Project(IDocumentSession session)
                {
                    var dtos = session.Query<Book, BooksByAuthor>()
                        .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
                        .ProjectInto<BookDto>();
                }
            }
            """;

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Query_Field_Rule_Is_Reported_Without_A_Compilation_End_Pass()
        {
            ImmutableArray<Diagnostic> diagnostics = await EditorDiagnosticsAsync<QueryIndexFieldAnalyzer>();

            Assert.Contains(diagnostics, d => d.Id == "RVN007");
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Projection_Rule_Is_Reported_Without_A_Compilation_End_Pass()
        {
            ImmutableArray<Diagnostic> diagnostics = await EditorDiagnosticsAsync<QueryProjectionFieldAnalyzer>();

            Assert.Contains(diagnostics, d => d.Id == "RVN008");
        }

        /// <summary>
        /// Collects diagnostics the way an editor does: semantic analysis of one file, with no
        /// compilation-end pass.
        /// </summary>
        private static async Task<ImmutableArray<Diagnostic>> EditorDiagnosticsAsync<TAnalyzer>()
            where TAnalyzer : DiagnosticAnalyzer, new()
        {
            CSharpParseOptions parseOptions = new(LanguageVersion.Preview);
            SyntaxTree tree = CSharpSyntaxTree.ParseText(Source, parseOptions);

            CSharpCompilation compilation = CSharpCompilation.Create(
                "TestAssembly",
                [tree],
                RavenAnalyzerTest.MetadataReferences,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

            CSharpGeneratorDriver.Create(
                    [new Raven.Analyzers.Generators.IndexMetadataGenerator().AsSourceGenerator()],
                    parseOptions: parseOptions)
                .RunGeneratorsAndUpdateCompilation(compilation, out Compilation generated, out _);

            compilation = (CSharpCompilation)generated;

            // The tree the editor would have open. Generated trees are appended, so the original is still
            // first, and it is the one carrying the query under test.
            SyntaxTree openTree = compilation.SyntaxTrees.First();

            CompilationWithAnalyzers withAnalyzers = compilation.WithAnalyzers(
                ImmutableArray.Create<DiagnosticAnalyzer>(new TAnalyzer()));

            return await withAnalyzers.GetAnalyzerSemanticDiagnosticsAsync(
                compilation.GetSemanticModel(openTree), filterSpan: null, cancellationToken: default);
        }
    }
}
