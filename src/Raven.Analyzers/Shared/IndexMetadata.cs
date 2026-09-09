using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;

namespace Raven.Analyzers.Shared
{
    /// <summary>
    /// The statically-known shape of one index class, covering its whole inheritance chain. Read back
    /// from the <c>RavenIndexMetadataAttribute</c> that the generator emits into the assembly declaring
    /// the index, so the values are the same whether the index came from source or a referenced DLL.
    /// </summary>
    /// <remarks>
    /// <see cref="Analyzable"/> false means "unknown", never "empty". Every consumer must bail on it:
    /// treating an unreadable index as having no fields would make every queried field look unindexed.
    /// </remarks>
    internal sealed record IndexMetadata(
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
        /// the shape is unknown, so callers must not report anything that depends on it. A single shared
        /// instance, since this is by far the most frequently returned value.
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
    /// Reads <see cref="IndexMetadata"/> for index symbols. One instance per compilation.
    /// </summary>
    /// <remarks>
    /// Metadata for an index always sits on the assembly that declares it, so a lookup only ever needs
    /// that one assembly's attributes. Those are indexed by type the first time an assembly is touched,
    /// which keeps a lookup at one dictionary hit. Scanning the attribute list per index instead would
    /// cost O(indexes squared) in an assembly that declares many of them, since each scan walks every
    /// recorded entry to find one.
    /// </remarks>
    internal sealed class IndexMetadataRegistry
    {
        // Analyzers run concurrently over one compilation, so both the outer map and the lazily built
        // per-assembly maps have to be safe to reach from several threads at once. The inner maps are
        // fully populated before being published, and never mutated afterwards.
        private readonly ConcurrentDictionary<IAssemblySymbol, IReadOnlyDictionary<INamedTypeSymbol, IndexMetadata>> _byAssembly =
            new(SymbolEqualityComparer.Default);

        public IndexMetadata Read(INamedTypeSymbol indexClass)
        {
            IReadOnlyDictionary<INamedTypeSymbol, IndexMetadata> recorded =
                _byAssembly.GetOrAdd(indexClass.ContainingAssembly, BuildFor);

            // Generic index classes are recorded by their open definition, because that is what
            // typeof(Foo<>) binds to in the generated attribute.
            return recorded.TryGetValue(indexClass.OriginalDefinition, out IndexMetadata? metadata)
                ? metadata
                : IndexMetadata.Unknown;
        }

        private static IReadOnlyDictionary<INamedTypeSymbol, IndexMetadata> BuildFor(IAssemblySymbol assembly)
        {
            Dictionary<INamedTypeSymbol, IndexMetadata> result = new(SymbolEqualityComparer.Default);

            foreach (AttributeData attribute in assembly.GetAttributes())
            {
                if (attribute.AttributeClass?.ToDisplayString() != KnownTypes.RavenIndexMetadataAttributeFullName)
                    continue;

                if (attribute.ConstructorArguments.Length != 1)
                    continue;

                if (attribute.ConstructorArguments[0].Value is not INamedTypeSymbol recordedType)
                    continue;

                result[recordedType.OriginalDefinition] = Parse(attribute);
            }

            return result;
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

            foreach (KeyValuePair<string, TypedConstant> named in attribute.NamedArguments)
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

            // A recorded but unable to be analyzed index carries no usable values; collapse it to Unknown so
            // there is a single "cannot reason about this" value for callers to test.
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
