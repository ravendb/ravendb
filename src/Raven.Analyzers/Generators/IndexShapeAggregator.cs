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
    /// Runs in the assembly that declares the index, which is the only place the constructor bodies
    /// exist. Every recorded value already describes a full chain, so splicing one base's metadata ends
    /// the walk; there is no need to climb further into assemblies that may not be readable either.
    /// </remarks>
    internal static class IndexShapeAggregator
    {
        public static IndexMetadata Compute(
            INamedTypeSymbol indexClass,
            Compilation compilation,
            IndexMetadataRegistry metadataRegistry)
        {
            // A JavaScript index defines its maps as strings; nothing about its shape is knowable from
            // the C# side, so record it as unanalyzable rather than as an index with no fields.
            if (SyntaxHelpers.IsJavaScriptIndex(indexClass))
                return IndexMetadata.Unknown;

            if (!SyntaxHelpers.DefinesShapeThroughMap(indexClass))
                return IndexMetadata.Unknown;

            IndexInheritanceInspector.TryCollectChainDeclarations(
                indexClass, compilation, out List<ClassDeclarationSyntax> declarations, out INamedTypeSymbol? foreignBase);

            IndexFieldSet mapFields = IndexFieldExtractor.ExtractLocalPrefix(indexClass, compilation, out _);
            IndexStoredFieldSet storedFields = IndexStoredFieldExtractor.ExtractLocalPrefix(indexClass, compilation, out _);
            bool fieldsKnown = mapFields.Status != IndexFieldInspection.BailCannotAnalyze
                               && storedFields.Status != StoredFieldsStatus.BailCannotAnalyze;

            // The readable prefix's own Map / AddMap facts. Both walkers report "unknown" when they hit
            // an unreadable base, which is precisely the case the inherited metadata below fills in, so
            // an unknown here is not itself fatal.
            IndexChainSearch mapSearch = IndexInheritanceInspector.FindMapAssignmentInChain(indexClass, compilation);
            IndexChainSearch reduceSearch = IndexInheritanceInspector.FindAssignmentInChain(indexClass, compilation, KnownTypes.ReduceFieldName);
            (int addMapCount, bool addMapInLoop, bool usesAddMapForAll, bool addMapUnknown) =
                IndexInheritanceInspector.CountAddMapInChain(indexClass, compilation);

            IndexMetadata local = new(
                Analyzable: true,
                FieldsKnown: fieldsKnown,
                MapFields: fieldsKnown ? mapFields.Fields : ImmutableHashSet<string>.Empty,
                StoredFields: fieldsKnown ? storedFields.Fields : ImmutableHashSet<string>.Empty,
                StoreAllFields: fieldsKnown && storedFields.Status == StoredFieldsStatus.AllStored,
                AssignsMap: mapSearch == IndexChainSearch.Found,
                IsMapReduce: reduceSearch == IndexChainSearch.Found,
                AddMapCount: addMapCount,
                AddMapInLoop: addMapInLoop,
                UsesAddMapForAll: usesAddMapForAll,
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
            IndexMetadata inherited = metadataRegistry.Read(foreignBase);
            if (!inherited.Analyzable)
                return IndexMetadata.Unknown;

            return Merge(local, inherited);
        }

        private static IndexMetadata Merge(IndexMetadata local, IndexMetadata inherited) => new(
            Analyzable: true,
            FieldsKnown: local.FieldsKnown && inherited.FieldsKnown,
            MapFields: local.MapFields.Union(inherited.MapFields),
            StoredFields: local.StoredFields.Union(inherited.StoredFields),
            StoreAllFields: local.StoreAllFields || inherited.StoreAllFields,
            AssignsMap: local.AssignsMap || inherited.AssignsMap,
            IsMapReduce: local.IsMapReduce || inherited.IsMapReduce,
            AddMapCount: local.AddMapCount + inherited.AddMapCount,
            AddMapInLoop: local.AddMapInLoop || inherited.AddMapInLoop,
            UsesAddMapForAll: local.UsesAddMapForAll || inherited.UsesAddMapForAll,
            UsesAdditionalCode: local.UsesAdditionalCode || inherited.UsesAdditionalCode);
    }
}
