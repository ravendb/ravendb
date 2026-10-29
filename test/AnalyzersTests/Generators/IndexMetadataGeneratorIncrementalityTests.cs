using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Raven.Analyzers.Generators;
using Tests.Infrastructure;
using Xunit;

namespace AnalyzersTests.Generators
{
    /// <summary>
    /// Pins the generator's incrementality: editing a file that declares no index must not re-run the
    /// per-class extraction.
    /// </summary>
    /// <remarks>
    /// Extraction walks constructors and resolves symbols, so it is much too expensive to redo on every
    /// keystroke in an IDE. Roslyn will reuse the previous result, but only while two conditions hold,
    /// and both are easy to break by accident: the step must not depend on CompilationProvider, which
    /// reports a new value for any change at all, and the value the step produces must compare by
    /// content rather than by reference, which rules out passing symbols through the pipeline. Neither
    /// condition shows up as a build or test failure if it regresses; the generator simply gets slow.
    /// Asserting on the recorded step reasons is what makes that visible.
    /// </remarks>
    public class IndexMetadataGeneratorIncrementalityTests
    {
        private const string IndexSource = """
            using System.Linq;
            using Raven.Client.Documents.Indexes;

            namespace App
            {
                public class Book
                {
                    public string Id { get; set; }
                    public string AuthorId { get; set; }
                }

                public class BooksByAuthor : AbstractIndexCreationTask<Book>
                {
                    public BooksByAuthor()
                    {
                        Map = books => from book in books select new { book.AuthorId };
                    }
                }
            }
            """;

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Editing_An_Unrelated_File_Reuses_The_Extraction_Step()
        {
            const string before = "namespace App { public static class Unrelated { public static int Value => 1; } }";
            const string after = "namespace App { public static class Unrelated { public static int Value => 2; } }";

            StepReasons reasons = RunTwiceAndGetSecondRunReasons(before, after);

            Assert.NotEmpty(reasons.Extraction);
            Assert.All(reasons.Extraction, reason =>
                Assert.True(
                    reason is IncrementalStepRunReason.Cached or IncrementalStepRunReason.Unchanged,
                    $"Extraction re-ran with reason '{reason}' after an edit to a file declaring no index. "
                    + "Its value no longer compares by content, so Roslyn cannot reuse the previous run."));

            // The output step is what a CompilationProvider dependency shows up in. Extraction stays
            // cached in that case, because the dependency sits downstream of it, so asserting only on
            // extraction would miss it entirely.
            Assert.NotEmpty(reasons.Output);
            Assert.All(reasons.Output, reason =>
                Assert.True(
                    reason is IncrementalStepRunReason.Cached or IncrementalStepRunReason.Unchanged,
                    $"The source output re-ran with reason '{reason}' after an edit to a file declaring no "
                    + "index. Something in the pipeline now depends on the whole compilation."));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Editing_The_Index_Itself_Re_Runs_The_Extraction_Step()
        {
            // The counterpart: caching must not be so eager that a real change to the index is missed.
            string changed = IndexSource.Replace("new { book.AuthorId }", "new { book.AuthorId, book.Id }");

            StepReasons reasons =
                RunTwiceAndGetSecondRunReasons(unrelated: "namespace App { public static class Unrelated { } }",
                                               unrelatedAfter: "namespace App { public static class Unrelated { } }",
                                               indexAfter: changed);

            Assert.Contains(IncrementalStepRunReason.Modified, reasons.Extraction);
        }

        private sealed record StepReasons(
            ImmutableArray<IncrementalStepRunReason> Extraction,
            ImmutableArray<IncrementalStepRunReason> Output);

        private static StepReasons RunTwiceAndGetSecondRunReasons(
            string unrelated,
            string unrelatedAfter,
            string? indexAfter = null)
        {
            CSharpParseOptions parseOptions = new(LanguageVersion.Preview);

            SyntaxTree indexTree = CSharpSyntaxTree.ParseText(IndexSource, parseOptions, path: "Index.cs");
            SyntaxTree unrelatedTree = CSharpSyntaxTree.ParseText(unrelated, parseOptions, path: "Unrelated.cs");

            CSharpCompilation compilation = CSharpCompilation.Create(
                "TestAssembly",
                [indexTree, unrelatedTree],
                RavenAnalyzerTest.MetadataReferences,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

            GeneratorDriver driver = CSharpGeneratorDriver.Create(
                [new IndexMetadataGenerator().AsSourceGenerator()],
                parseOptions: parseOptions,
                optionsProvider: null,
                driverOptions: new GeneratorDriverOptions(
                    IncrementalGeneratorOutputKind.None,
                    trackIncrementalGeneratorSteps: true));

            driver = driver.RunGenerators(compilation);

            // Replace the trees rather than build a new compilation, so the driver sees an edit to an
            // existing document, which is what an IDE does as the user types.
            CSharpCompilation edited = compilation.ReplaceSyntaxTree(
                unrelatedTree, CSharpSyntaxTree.ParseText(unrelatedAfter, parseOptions, path: "Unrelated.cs"));

            if (indexAfter != null)
            {
                edited = edited.ReplaceSyntaxTree(
                    indexTree, CSharpSyntaxTree.ParseText(indexAfter, parseOptions, path: "Index.cs"));
            }

            driver = driver.RunGenerators(edited);

            GeneratorDriverRunResult result = driver.GetRunResult();

            return new StepReasons(
                Extraction: Reasons(result, r => r.TrackedSteps
                    .Where(step => step.Key == IndexMetadataGenerator.ShapesStepName)
                    .SelectMany(step => step.Value)),
                Output: Reasons(result, r => r.TrackedOutputSteps.SelectMany(step => step.Value)));
        }

        private static ImmutableArray<IncrementalStepRunReason> Reasons(
            GeneratorDriverRunResult result,
            System.Func<GeneratorRunResult, IEnumerable<IncrementalGeneratorRunStep>> select) =>
            result.Results
                .SelectMany(select)
                .SelectMany(step => step.Outputs)
                .Select(output => output.Reason)
                .ToImmutableArray();
    }
}
