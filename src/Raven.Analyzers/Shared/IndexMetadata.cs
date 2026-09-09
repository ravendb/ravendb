using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;

namespace Raven.Analyzers.Shared
{
    /// <summary>
    /// The statically-known shape of one index class, covering its whole inheritance chain. Read back
    /// from the <c>RavenIndexMetadataAttribute</c> that the generator emits into the assembly declaring
    /// the index, so the values are identical whether the index came from source or a referenced DLL.
    /// </summary>
    /// <remarks>
    /// <see cref="Analyzable"/> false means "unknown", never "empty". Every consumer must bail on it:
    /// treating an unreadable index as having no fields would make every queried field look unindexed.
    /// </remarks>
    internal readonly record struct IndexMetadata(
        bool Analyzable,
        ImmutableHashSet<string> MapFields,
        ImmutableHashSet<string> StoredFields,
        bool StoreAllFields,
        bool AssignsMap,
        int AddMapCount,
        bool AddMapInLoop,
        bool UsesAdditionalCode)
    {
        /// <summary>
        /// No metadata was found, or the generator recorded that it could not read the index. Both mean
        /// the shape is unknown, so callers must not report anything that depends on it.
        /// </summary>
        public static readonly IndexMetadata Unknown = new(
            Analyzable: false,
            MapFields: ImmutableHashSet<string>.Empty,
            StoredFields: ImmutableHashSet<string>.Empty,
            StoreAllFields: false,
            AssignsMap: false,
            AddMapCount: 0,
            AddMapInLoop: false,
            UsesAdditionalCode: false);
    }

    /// <summary>
    /// Reads <see cref="IndexMetadata"/> for an index symbol out of the assembly attributes of the
    /// assembly that declares it.
    /// </summary>
    /// <remarks>
    /// Only the declaring assembly is inspected, never the full reference closure: the generator always
    /// emits an index's metadata into the assembly that declares it, so a single
    /// <see cref="ISymbol.ContainingAssembly"/> lookup suffices and the cost stays proportional to the
    /// indexes actually queried rather than to the number of references.
    /// </remarks>
    internal static class IndexMetadataReader
    {
        /// <summary>
        /// Creates a per-compilation cache. Metadata for one index is looked up at most once even when
        /// many queries target it; analyzers run concurrently, hence the concurrent dictionary.
        /// </summary>
        public static ConcurrentDictionary<INamedTypeSymbol, IndexMetadata> CreateCache() =>
            new(SymbolEqualityComparer.Default);

        public static IndexMetadata Read(
            INamedTypeSymbol indexClass,
            ConcurrentDictionary<INamedTypeSymbol, IndexMetadata> cache) =>
            cache.GetOrAdd(indexClass, Read);

        public static IndexMetadata Read(INamedTypeSymbol indexClass)
        {
            // Generic index classes are recorded by their open definition, because that is what
            // typeof(Foo<>) binds to in the generated attribute.
            INamedTypeSymbol target = indexClass.OriginalDefinition;

            foreach (AttributeData attribute in indexClass.ContainingAssembly.GetAttributes())
            {
                if (attribute.AttributeClass?.ToDisplayString() != KnownTypes.RavenIndexMetadataAttributeFullName)
                    continue;

                if (attribute.ConstructorArguments.Length != 1)
                    continue;

                if (attribute.ConstructorArguments[0].Value is not INamedTypeSymbol recordedType)
                    continue;

                if (!SymbolEqualityComparer.Default.Equals(recordedType.OriginalDefinition, target))
                    continue;

                return Parse(attribute);
            }

            return IndexMetadata.Unknown;
        }

        private static IndexMetadata Parse(AttributeData attribute)
        {
            bool analyzable = false;
            ImmutableHashSet<string> mapFields = ImmutableHashSet<string>.Empty;
            ImmutableHashSet<string> storedFields = ImmutableHashSet<string>.Empty;
            bool storeAllFields = false;
            bool assignsMap = false;
            int addMapCount = 0;
            bool addMapInLoop = false;
            bool usesAdditionalCode = false;

            foreach (System.Collections.Generic.KeyValuePair<string, TypedConstant> named in attribute.NamedArguments)
            {
                switch (named.Key)
                {
                    case nameof(IndexMetadata.Analyzable):
                        analyzable = named.Value.Value is true;
                        break;
                    case nameof(IndexMetadata.MapFields):
                        mapFields = ToStringSet(named.Value);
                        break;
                    case nameof(IndexMetadata.StoredFields):
                        storedFields = ToStringSet(named.Value);
                        break;
                    case nameof(IndexMetadata.StoreAllFields):
                        storeAllFields = named.Value.Value is true;
                        break;
                    case nameof(IndexMetadata.AssignsMap):
                        assignsMap = named.Value.Value is true;
                        break;
                    case nameof(IndexMetadata.AddMapCount):
                        addMapCount = named.Value.Value is int count ? count : 0;
                        break;
                    case nameof(IndexMetadata.AddMapInLoop):
                        addMapInLoop = named.Value.Value is true;
                        break;
                    case nameof(IndexMetadata.UsesAdditionalCode):
                        usesAdditionalCode = named.Value.Value is true;
                        break;
                }
            }

            // A recorded-but-unanalyzable index carries no usable values; collapse it to Unknown so
            // there is a single "cannot reason about this" representation for callers to test.
            return analyzable
                ? new IndexMetadata(true, mapFields, storedFields, storeAllFields, assignsMap, addMapCount, addMapInLoop, usesAdditionalCode)
                : IndexMetadata.Unknown;
        }

        private static ImmutableHashSet<string> ToStringSet(TypedConstant value)
        {
            if (value.Kind != TypedConstantKind.Array || value.Values.IsDefaultOrEmpty)
                return ImmutableHashSet<string>.Empty;

            return value.Values
                .Select(v => v.Value as string)
                .Where(s => string.IsNullOrEmpty(s) == false)
                .ToImmutableHashSet(System.StringComparer.Ordinal)!;
        }
    }
}
