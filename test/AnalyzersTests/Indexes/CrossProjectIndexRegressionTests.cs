using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers.Indexes;
using Raven.Analyzers.Queries;
using Tests.Infrastructure;
using Xunit;

namespace AnalyzersTests.Indexes
{
    /// <summary>
    /// Covers indexes that live in a different project from the code being analysed, the layout where
    /// index classes sit in their own assembly and queries reference them.
    /// </summary>
    /// <remarks>
    /// Every other test in this suite puts the index and the query in one compilation, which hides two
    /// distinct failures. An index's shape is defined in its constructor body, and constructor bodies do
    /// not survive into compiled metadata, so a rule that read the constructor directly could see nothing
    /// through a compiled project reference. Inside an IDE the same reference carries syntax owned by
    /// another compilation, and asking the analysed compilation for a semantic model over it throws,
    /// which surfaces as AD0001 and silently costs every diagnostic from that analyzer. Reading the
    /// shape from the recorded metadata attribute avoids both, and these tests run each case through a
    /// compiled DLL and a compilation reference to prove the two hosts agree.
    /// </remarks>
    public class CrossProjectIndexRegressionTests
    {
        private const string IndexProject = """
            using System.Linq;
            using Raven.Client.Documents.Indexes;

            namespace Model
            {
                public class Book
                {
                    public string Id { get; set; }
                    public string Title { get; set; }
                    public string AuthorId { get; set; }
                }

                public class BooksByAuthor : AbstractIndexCreationTask<Book>
                {
                    public BooksByAuthor()
                    {
                        Map = books => from book in books select new { book.AuthorId };
                    }
                }

                public abstract class BookIndexBase : AbstractIndexCreationTask<Book>
                {
                    protected BookIndexBase()
                    {
                        Map = books => from book in books select new { book.AuthorId };
                    }
                }

                // Deliberately assigns no Map, so a derived index that adds none either has none at all.
                public abstract class MaplessBookIndexBase : AbstractIndexCreationTask<Book>
                {
                    protected MaplessBookIndexBase()
                    {
                    }
                }

                public class BooksByAuthorStoringNothing : AbstractIndexCreationTask<Book>
                {
                    public BooksByAuthorStoringNothing()
                    {
                        Map = books => from book in books select new { book.AuthorId };
                        StoreAllFields(FieldStorage.No);
                    }
                }

                // Ships C# for the server to compile, which must stand down the untranslatable-call rule
                // for anything deriving from it.
                public abstract class BookIndexShippingCode : AbstractIndexCreationTask<Book>
                {
                    protected BookIndexShippingCode()
                    {
                        AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                        {
                            { "Helper", "public static class Helper { public static string Norm(string s) => s; }" }
                        };
                    }
                }
            }
            """;

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(ReferenceKind.CompiledDll)]
        [InlineData(ReferenceKind.CompilationReference)]
        [InlineData(ReferenceKind.CompilationReferenceWithoutGeneratedOutput)]
        public async Task Query_On_Field_Missing_From_Referenced_Index_Is_Reported(
            ReferenceKind referenceKind)
        {
            const string source = """
                using System.Linq;
                using Model;
                using Raven.Client.Documents;
                using Raven.Client.Documents.Linq;
                using Raven.Client.Documents.Session;

                public static class Queries
                {
                    public static void Filter(IDocumentSession session)
                    {
                        var books = session.Query<Book, BooksByAuthor>()
                            .Where(book => book.Title == "test")
                            .Take(4)
                            .ToArray();
                    }
                }
                """;

            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeWithReferencedProjectAsync<QueryIndexFieldAnalyzer>(
                    IndexProject, source, referenceKind);

            Diagnostic diagnostic = Assert.Single(diagnostics);
            Assert.Equal("RVN007", diagnostic.Id);
            Assert.Contains("Title", diagnostic.GetMessage());
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(ReferenceKind.CompiledDll)]
        [InlineData(ReferenceKind.CompilationReference)]
        [InlineData(ReferenceKind.CompilationReferenceWithoutGeneratedOutput)]
        public async Task Query_On_Field_Present_In_Referenced_Index_Is_Not_Reported(
            ReferenceKind referenceKind)
        {
            const string source = """
                using System.Linq;
                using Model;
                using Raven.Client.Documents;
                using Raven.Client.Documents.Linq;
                using Raven.Client.Documents.Session;

                public static class Queries
                {
                    public static void Filter(IDocumentSession session)
                    {
                        var books = session.Query<Book, BooksByAuthor>()
                            .Where(book => book.AuthorId == "authors/1")
                            .Take(4)
                            .ToArray();
                    }
                }
                """;

            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeWithReferencedProjectAsync<QueryIndexFieldAnalyzer>(
                    IndexProject, source, referenceKind);

            Assert.Empty(diagnostics);
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(ReferenceKind.CompiledDll)]
        [InlineData(ReferenceKind.CompilationReference)]
        [InlineData(ReferenceKind.CompilationReferenceWithoutGeneratedOutput)]
        public async Task Index_Inheriting_Map_From_Referenced_Base_Does_Not_Report_Missing_Map(
            ReferenceKind referenceKind)
        {
            // The Map lives in the referenced base class, so the derived index has one and RVN004 must
            // stay silent. Reporting here would be a false positive on a perfectly valid index.
            const string source = """
                using Model;

                public class BooksByAuthorDerived : BookIndexBase
                {
                    public BooksByAuthorDerived()
                    {
                    }
                }
                """;

            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeWithReferencedProjectAsync<IndexDefinitionAnalyzer>(
                    IndexProject, source, referenceKind);

            Assert.Empty(diagnostics);
        }
        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(ReferenceKind.CompiledDll)]
        [InlineData(ReferenceKind.CompilationReference)]
        [InlineData(ReferenceKind.CompilationReferenceWithoutGeneratedOutput)]
        public async Task Index_Whose_Referenced_Base_Assigns_No_Map_Is_Reported(
            ReferenceKind referenceKind)
        {
            // The counterpart to the inherited-Map case: this chain genuinely has no Map, and the recorded
            // metadata says so, which is the only way to tell it apart from a base whose Map cannot be
            // read. Suppressing on "a base exists that I cannot inspect" would miss this entirely.
            const string source = """
                using Model;

                public class BooksByAuthorWithoutMap : MaplessBookIndexBase
                {
                    public BooksByAuthorWithoutMap()
                    {
                    }
                }
                """;

            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeWithReferencedProjectAsync<IndexDefinitionAnalyzer>(
                    IndexProject, source, referenceKind);

            Diagnostic diagnostic = Assert.Single(diagnostics);
            Assert.Equal("RVN004", diagnostic.Id);
            Assert.Contains("BooksByAuthorWithoutMap", diagnostic.GetMessage());
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(ReferenceKind.CompiledDll)]
        [InlineData(ReferenceKind.CompilationReference)]
        [InlineData(ReferenceKind.CompilationReferenceWithoutGeneratedOutput)]
        public async Task Projection_Of_Field_Not_Stored_By_Referenced_Index_Is_Reported(
            ReferenceKind referenceKind)
        {
            // Stored-field facts have to survive the assembly boundary too, not just map fields.
            const string source = """
                using System.Linq;
                using Model;
                using Raven.Client.Documents;
                using Raven.Client.Documents.Linq;
                using Raven.Client.Documents.Queries;
                using Raven.Client.Documents.Session;

                public class BookDto { public string AuthorId { get; set; } }

                public static class Queries
                {
                    public static void Project(IDocumentSession session)
                    {
                        var dtos = session.Query<Book, BooksByAuthorStoringNothing>()
                            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
                            .ProjectInto<BookDto>();
                    }
                }
                """;

            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeWithReferencedProjectAsync<QueryProjectionFieldAnalyzer>(
                    IndexProject, source, referenceKind);

            Diagnostic diagnostic = Assert.Single(diagnostics);
            Assert.Equal("RVN008", diagnostic.Id);
            Assert.Contains("AuthorId", diagnostic.GetMessage());
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(ReferenceKind.CompiledDll)]
        [InlineData(ReferenceKind.CompilationReference)]
        [InlineData(ReferenceKind.CompilationReferenceWithoutGeneratedOutput)]
        public async Task Helper_Call_Is_Not_Reported_When_Referenced_Base_Ships_Server_Side_Code(
            ReferenceKind referenceKind)
        {
            // The AdditionalSources write lives in the referenced base while the Map that calls the helper
            // lives here, so the suppression fact has to cross the assembly boundary. Without it this is a
            // false positive on an index that is perfectly valid.
            const string source = """
                using System.Linq;
                using Model;

                public class BooksByNormalizedAuthor : BookIndexShippingCode
                {
                    public BooksByNormalizedAuthor()
                    {
                        Map = books => from book in books select new { AuthorId = Normalize(book.AuthorId) };
                    }

                    private static string Normalize(string value) => value;
                }
                """;

            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeWithReferencedProjectAsync<IndexUnsupportedMethodAnalyzer>(
                    IndexProject, source, referenceKind);

            Assert.Empty(diagnostics);
        }
    }
}
