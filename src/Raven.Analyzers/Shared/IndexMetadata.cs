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
        /// <summary>
        /// Bound on how far the source fallback below follows project references. Chains are one or two
        /// hops in practice; the cap only exists so a pathological reference graph cannot recurse forever.
        /// </summary>
        private const int MaxReferenceDepth = 8;

        // Analyzers run concurrently over one compilation, so every map here has to be safe to reach from
        // several threads at once. The per-assembly maps are fully populated before being published, and
        // never mutated afterwards.
        private readonly ConcurrentDictionary<IAssemblySymbol, IReadOnlyDictionary<INamedTypeSymbol, IndexMetadata>> _byAssembly =
            new(SymbolEqualityComparer.Default);

        private readonly ConcurrentDictionary<INamedTypeSymbol, IndexMetadata> _fromSource =
            new(SymbolEqualityComparer.Default);

        private readonly Compilation _compilation;
        private readonly int _depth;

        public IndexMetadataRegistry(Compilation compilation)
            : this(compilation, depth: 0)
        {
        }

        private IndexMetadataRegistry(Compilation compilation, int depth)
        {
            _compilation = compilation;
            _depth = depth;
        }

        public IndexMetadata Read(INamedTypeSymbol indexClass)
        {
            IReadOnlyDictionary<INamedTypeSymbol, IndexMetadata> recorded =
                _byAssembly.GetOrAdd(indexClass.ContainingAssembly, BuildFor);

            // Generic index classes are recorded by their open definition, because that is what
            // typeof(Foo<>) binds to in the generated attribute.
            if (recorded.TryGetValue(indexClass.OriginalDefinition, out IndexMetadata? metadata))
                return metadata;

            return ReadFromReferencedSource(indexClass);
        }

        /// <summary>
        /// Last resort when no metadata was recorded for <paramref name="indexClass"/>: read the index out
        /// of the source of the compilation that declares it.
        /// </summary>
        /// <remarks>
        /// A command-line build never needs this. Every project is compiled to a DLL, the generator runs as
        /// part of that compile, and the attribute is physically present in the metadata the referencing
        /// project reads.
        /// <para>
        /// An IDE does need it. It does not compile the referenced project to a DLL; it hands the compilation
        /// under analysis an in-memory view of it as a <see cref="CompilationReference"/>, and that view need
        /// not include the referenced project's generated code, in which case the attribute is simply absent.
        /// The source is present though, which is the one thing an IDE always has, and a
        /// <see cref="CompilationReference"/> exposes the compilation that owns it, so the shape can be read
        /// the same way the generator would have read it.
        /// </para>
        /// Without this, every rule that depends on recorded index facts stays silent across a project
        /// boundary in the IDE while working correctly on the command line.
        /// </remarks>
        private IndexMetadata ReadFromReferencedSource(INamedTypeSymbol indexClass)
        {
            if (_depth >= MaxReferenceDepth)
                return IndexMetadata.Unknown;

            if (SymbolEqualityComparer.Default.Equals(indexClass.ContainingAssembly, _compilation.Assembly))
            {
                // Declared in the compilation being analysed, where the generator runs. A missing entry
                // means it deliberately recorded nothing, so re-reading the source here would only
                // disagree with what the build concluded.
                return IndexMetadata.Unknown;
            }

            if (_compilation.GetMetadataReference(indexClass.ContainingAssembly) is not CompilationReference reference)
                return IndexMetadata.Unknown; // a compiled DLL carries no source to fall back to

            return _fromSource.GetOrAdd(
                indexClass,
                symbol => Generators.IndexShapeAggregator.Compute(
                    symbol,
                    reference.Compilation,
                    new IndexMetadataRegistry(reference.Compilation, _depth + 1)));
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
