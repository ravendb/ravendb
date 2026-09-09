using System.Collections.Generic;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Analyzers.Indexes;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Generators
{
    /// <summary>
    /// Computes the complete <see cref="IndexMetadata"/> for one index class, covering its whole
    /// inheritance chain, by reading the part of the chain declared in this compilation and splicing on
    /// the recorded metadata of the first base class it cannot read.
    /// </summary>
    /// <remarks>
    /// This is the generator's half of the design: it runs in the assembly that declares the index,
    /// where the constructor bodies are available, and produces the values that
    /// <c>RavenIndexMetadataAttribute</c> carries into compiled metadata for every other assembly to
    /// read back. Because each recorded value already describes a full chain, splicing one base's
    /// metadata terminates the walk — there is no need to reach further up into assemblies that may
    /// themselves be unreadable.
    /// </remarks>
    internal static class IndexShapeAggregator
    {
        /// <summary>
        /// The shape of an index whose chain contributes nothing: the identity for the merge below, and
        /// what an index sitting directly on a framework base starts from.
        /// </summary>
        private static readonly IndexMetadata Empty = new(
            Analyzable: true,
            MapFields: ImmutableHashSet<string>.Empty,
            StoredFields: ImmutableHashSet<string>.Empty,
            StoreAllFields: false,
            AssignsMap: false,
            AddMapCount: 0,
            AddMapInLoop: false,
            UsesAdditionalCode: false);

        public static IndexMetadata Compute(INamedTypeSymbol indexClass, Compilation compilation)
        {
            // A JavaScript index defines its maps as strings; nothing about its shape is knowable from
            // the C# side, so record it as unanalyzable rather than as an index with no fields.
            if (SyntaxHelpers.IsJavaScriptIndex(indexClass))
                return IndexMetadata.Unknown;

            IndexInheritanceInspector.TryCollectChainDeclarations(
                indexClass, compilation, out List<ClassDeclarationSyntax> declarations, out INamedTypeSymbol? foreignBase);

            IndexFieldSet mapFields = IndexFieldExtractor.ExtractLocalPrefix(indexClass, compilation, out _);
            if (mapFields.Status == IndexFieldInspection.BailCannotAnalyze)
                return IndexMetadata.Unknown;

            IndexStoredFieldSet storedFields = IndexStoredFieldExtractor.ExtractLocalPrefix(indexClass, compilation, out _);
            if (storedFields.Status == StoredFieldsStatus.BailCannotAnalyze)
                return IndexMetadata.Unknown;

            // The readable prefix's own Map / AddMap facts. Both walkers report "unknown" when they hit
            // an unreadable base, which is precisely the case the inherited metadata below fills in, so
            // an unknown here is not itself fatal.
            IndexChainSearch mapSearch = IndexInheritanceInspector.FindMapAssignmentInChain(indexClass, compilation);
            (int addMapCount, bool addMapInLoop, bool addMapUnknown) =
                IndexInheritanceInspector.CountAddMapInChain(indexClass, compilation);

            IndexMetadata local = new(
                Analyzable: true,
                MapFields: mapFields.Fields,
                StoredFields: storedFields.Fields,
                StoreAllFields: storedFields.Status == StoredFieldsStatus.AllStored,
                AssignsMap: mapSearch == IndexChainSearch.Found,
                AddMapCount: addMapCount,
                AddMapInLoop: addMapInLoop,
                UsesAdditionalCode: IndexAdditionalCodeExtractor.ShipsServerSideCode(declarations, compilation));

            if (foreignBase == null)
            {
                // Whole chain was readable, so an "unknown" from either walker would be a contradiction
                // we must not paper over: treat it as unanalyzable rather than guess.
                return mapSearch == IndexChainSearch.Unknown || addMapUnknown
                    ? IndexMetadata.Unknown
                    : local;
            }

            // The chain continues into an assembly whose source we cannot read. Its recorded metadata is
            // the only way to learn what it contributes; without it the shape is unknown, and reporting
            // "no fields" instead would make every field queried through this index look unindexed.
            IndexMetadata inherited = IndexMetadataReader.Read(foreignBase);
            if (!inherited.Analyzable)
                return IndexMetadata.Unknown;

            return Merge(local, inherited);
        }

        private static IndexMetadata Merge(IndexMetadata local, IndexMetadata inherited) => new(
            Analyzable: true,
            MapFields: local.MapFields.Union(inherited.MapFields),
            StoredFields: local.StoredFields.Union(inherited.StoredFields),
            StoreAllFields: local.StoreAllFields || inherited.StoreAllFields,
            AssignsMap: local.AssignsMap || inherited.AssignsMap,
            AddMapCount: local.AddMapCount + inherited.AddMapCount,
            AddMapInLoop: local.AddMapInLoop || inherited.AddMapInLoop,
            UsesAdditionalCode: local.UsesAdditionalCode || inherited.UsesAdditionalCode);
    }
}
