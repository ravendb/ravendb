using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Generators
{
    /// <summary>
    /// Emits one assembly-level <c>RavenIndexMetadataAttribute</c> per index class in the compilation,
    /// recording the fields it maps and stores and how it defines itself.
    /// </summary>
    /// <remarks>
    /// An index declares its shape in a constructor body. Constructor bodies are not part of compiled
    /// metadata, so an analyzer looking at a query has no way to inspect an index that came from a
    /// referenced assembly. Attributes are part of metadata, so recording the shape here, where the
    /// source is still available, is what makes those indexes analyzable at all. It also keeps a
    /// command-line build and an IDE in agreement, because both read these values rather than one
    /// reading source and the other reading nothing.
    /// <para>
    /// Abstract bases get an entry too. A derived index in another assembly needs its base's facts to
    /// tell whether the chain assigns a Map.
    /// </para>
    /// </remarks>
    [Generator(LanguageNames.CSharp)]
    public sealed class IndexMetadataGenerator : IIncrementalGenerator
    {
        private const string GeneratedFileName = "RavenIndexMetadata.g.cs";

        /// <summary>
        /// Name of the per-class extraction step. Costs nothing unless a driver is created with step
        /// tracking on, and lets a test assert that the step was reused rather than re-run. Public
        /// because the test project reaches it across the assembly boundary and this assembly has no
        /// InternalsVisibleTo bridge, matching how KnownTypes and DiagnosticIds are shared.
        /// </summary>
        public const string ShapesStepName = "RavenIndexShapes";

        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            // All of the expensive work happens in the transform, which Roslyn caches per syntax node and
            // re-runs only for nodes that actually changed. Two things are needed for that to hold: the
            // step must not depend on CompilationProvider, which reports a new value on every keystroke
            // and would drag the whole pipeline with it, and its result must be a value that compares by
            // content, which is what RecordedIndexShape is for. The Compilation the extraction needs
            // comes from the node's own semantic model instead.
            IncrementalValuesProvider<RecordedIndexShape> shapes = context.SyntaxProvider
                .CreateSyntaxProvider(
                    predicate: static (node, _) => node is ClassDeclarationSyntax { BaseList: not null },
                    transform: static (ctx, _) => Extract(ctx))
                .Where(static shape => shape is not null)
                .WithTrackingName(ShapesStepName)!;

            context.RegisterSourceOutput(shapes.Collect(), static (spc, collected) => Emit(spc, collected));
        }

        private static RecordedIndexShape? Extract(GeneratorSyntaxContext context)
        {
            var declaration = (ClassDeclarationSyntax)context.Node;

            if (context.SemanticModel.GetDeclaredSymbol(declaration) is not INamedTypeSymbol indexClass)
                return null;

            if (!SyntaxHelpers.IsIndexCreationTask(indexClass))
                return null;

            // Some index classes cannot be named by an assembly-level typeof() at all. Drop those here
            // rather than emit source that will not compile.
            if (!TryGetTypeOfExpression(indexClass, out string typeName))
                return null;

            // A fresh registry per extraction. It only builds an assembly's lookup when the chain leaves
            // this assembly and a base's recorded metadata has to be read, so an index whose chain is
            // wholly local never touches it. Rebuilding it for the indexes that do is affordable because
            // this whole step is cached: it re-runs when this one class changes, not on every edit.
            IndexMetadata metadata = IndexShapeAggregator.Compute(
                indexClass, context.SemanticModel.Compilation, new IndexMetadataRegistry());

            return new RecordedIndexShape(
                TypeName: typeName,
                Analyzable: metadata.Analyzable,
                MapFields: RenderFields(metadata.MapFields),
                StoredFields: RenderFields(metadata.StoredFields),
                StoreAllFields: metadata.StoreAllFields,
                AssignsMap: metadata.AssignsMap,
                AddMapCount: metadata.AddMapCount,
                AddMapInLoop: metadata.AddMapInLoop,
                UsesAdditionalCode: metadata.UsesAdditionalCode);
        }

        private static void Emit(SourceProductionContext context, ImmutableArray<RecordedIndexShape> shapes)
        {
            if (shapes.IsDefaultOrEmpty)
                return;

            var builder = new StringBuilder();
            builder.AppendLine("// <auto-generated/>");
            builder.AppendLine("// Index shapes recorded for the RavenDB analyzers. Do not edit.");
            builder.AppendLine();

            // A partial class produces one node per part, so the same shape arrives more than once.
            // Ordering by type name also keeps the file byte-identical between runs regardless of the
            // order the syntax nodes were visited in.
            IEnumerable<RecordedIndexShape> unique = shapes
                .GroupBy(shape => shape.TypeName, System.StringComparer.Ordinal)
                .Select(group => group.First())
                .OrderBy(shape => shape.TypeName, System.StringComparer.Ordinal);

            foreach (RecordedIndexShape shape in unique)
                AppendAttribute(builder, shape);

            context.AddSource(GeneratedFileName, SourceText.From(builder.ToString(), Encoding.UTF8));
        }

        private static void AppendAttribute(StringBuilder builder, RecordedIndexShape shape)
        {
            builder.Append("[assembly: global::")
                   .Append(KnownTypes.RavenIndexMetadataAttributeFullName)
                   .Append("(typeof(")
                   .Append(shape.TypeName)
                   .Append(')');

            // Unreadable indexes get an entry saying so rather than no entry at all. Both make a consumer
            // bail, but only an explicit false says the index was looked at and could not be read, which
            // is worth being able to tell apart when something is not reporting and you want to know why.
            builder.Append(", Analyzable = ").Append(shape.Analyzable ? "true" : "false");

            if (shape.Analyzable)
            {
                AppendFields(builder, nameof(IndexMetadata.MapFields), shape.MapFields);
                AppendFields(builder, nameof(IndexMetadata.StoredFields), shape.StoredFields);
                builder.Append(", StoreAllFields = ").Append(shape.StoreAllFields ? "true" : "false");
                builder.Append(", AssignsMap = ").Append(shape.AssignsMap ? "true" : "false");
                builder.Append(", AddMapCount = ").Append(shape.AddMapCount);
                builder.Append(", AddMapInLoop = ").Append(shape.AddMapInLoop ? "true" : "false");
                builder.Append(", UsesAdditionalCode = ").Append(shape.UsesAdditionalCode ? "true" : "false");
            }

            builder.AppendLine(")]");
        }

        private static void AppendFields(StringBuilder builder, string name, string renderedFields)
        {
            if (renderedFields.Length == 0)
                return;

            builder.Append(", ").Append(name).Append(" = new string[] { ").Append(renderedFields).Append(" }");
        }

        /// <summary>
        /// Renders a field set as the quoted, comma-joined contents of a <c>new string[] { … }</c>, or an
        /// empty string when there are none.
        /// </summary>
        private static string RenderFields(ImmutableHashSet<string> fields)
        {
            if (fields.IsEmpty)
                return string.Empty;

            // Sorted so the rendered value is stable. An unordered set would produce a different string
            // for the same fields, which would make this record compare unequal to the previous run and
            // defeat the caching the pipeline is built around.
            return string.Join(
                ", ",
                fields.OrderBy(f => f, System.StringComparer.Ordinal)
                      .Select(f => '"' + f.Replace("\\", "\\\\").Replace("\"", "\\\"") + '"'));
        }

        /// <summary>
        /// Builds the argument for a <c>typeof(...)</c> naming <paramref name="type"/>. Returns false when
        /// the type cannot be named from an assembly-level attribute.
        /// </summary>
        private static bool TryGetTypeOfExpression(INamedTypeSymbol type, out string expression)
        {
            expression = string.Empty;

            // A type parameter cannot appear in an assembly-level typeof(), and a query cannot target an
            // unbound generic index concretely anyway.
            if (type.IsGenericType || type.ContainingType?.IsGenericType == true)
                return false;

            if (type.DeclaredAccessibility == Accessibility.Private
                || type.DeclaredAccessibility == Accessibility.Protected)
            {
                return false;
            }

            expression = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            return true;
        }
    }
}
