using System;
using System.ComponentModel;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Records the statically-known shape of a single index class so the RavenDB analyzers can
    /// reason about it from any assembly. Emitted per index type at assembly level by the analyzer
    /// package's source generator; it is not meant to be written by hand.
    /// </summary>
    /// <remarks>
    /// An index defines its shape inside a constructor body (<c>Map = docs =&gt; …</c>,
    /// <c>AddMap&lt;T&gt;(…)</c>). Constructor bodies do not survive into compiled metadata, so an
    /// analyzer looking at a query in one assembly cannot read the index it targets in another.
    /// Attributes do survive, so the generator extracts the shape in the assembly that declares the
    /// index and records it here, where an analyzer can read it back through the ordinary symbol API
    /// whether the index came from source or from a referenced DLL.
    /// <para>
    /// Every value describes the whole inheritance chain of <see cref="IndexType"/>, not just its own
    /// declaration, so a consumer never has to walk base classes it may not be able to see.
    /// </para>
    /// </remarks>
    [AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
    // Public only because generated code in the user's own assembly has to name it; nobody writes it by
    // hand, so keep it out of completion lists. This hides the type from IntelliSense in projects that
    // reference Raven.Client, which is every consumer of it.
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class RavenIndexMetadataAttribute : Attribute
    {
        /// <summary>
        /// Creates metadata for <paramref name="indexType"/>. Callers set the remaining values through
        /// the properties; every one of them is meaningless unless <see cref="Analyzable"/> is true.
        /// </summary>
        public RavenIndexMetadataAttribute(Type indexType)
        {
            IndexType = indexType;
        }

        /// <summary>
        /// The index class this metadata describes.
        /// </summary>
        public Type IndexType { get; }

        /// <summary>
        /// Whether the generator could read the index completely. False when the shape cannot be
        /// determined statically — a JavaScript index, dynamic field creation via <c>CreateField</c>,
        /// a block-bodied map lambda, or a base class whose own metadata is missing or unanalyzable.
        /// Consumers must treat every other value here as unknown when this is false, rather than as
        /// an empty set, so that an unreadable index never produces a false report.
        /// </summary>
        public bool Analyzable { get; set; }

        /// <summary>
        /// Field names produced by the map projection across the chain. Empty when the index projects
        /// nothing statically knowable.
        /// </summary>
        public string[] MapFields { get; set; } = Array.Empty<string>();

        /// <summary>
        /// Field names explicitly stored across the chain, via <c>Store(…)</c>,
        /// <c>Stores[…] = FieldStorage.Yes</c>, or <c>StoresStrings[…] = FieldStorage.Yes</c>.
        /// Being part of the map projection does not make a field stored.
        /// </summary>
        public string[] StoredFields { get; set; } = Array.Empty<string>();

        /// <summary>
        /// Whether <c>StoreAllFields(FieldStorage.Yes)</c> was called anywhere in the chain, which makes
        /// every map-projection field stored regardless of <see cref="StoredFields"/>.
        /// </summary>
        public bool StoreAllFields { get; set; }

        /// <summary>
        /// Whether any constructor in the chain assigns the <c>Map</c> property.
        /// </summary>
        public bool AssignsMap { get; set; }

        /// <summary>
        /// Number of <c>AddMap</c> / <c>AddMapForAll</c> call sites across the constructors in the chain.
        /// </summary>
        public int AddMapCount { get; set; }

        /// <summary>
        /// Whether the chain writes to <c>AdditionalSources</c> or <c>AdditionalAssemblies</c>, shipping
        /// C# for the server to compile. A helper called from the map may then be translatable after all,
        /// so rules about untranslatable calls must stand down for such an index.
        /// </summary>
        public bool UsesAdditionalCode { get; set; }

        /// <summary>
        /// Whether any of those <c>AddMap</c> call sites sits inside a loop, in which case the call count
        /// understates how many maps are registered at runtime.
        /// </summary>
        public bool AddMapInLoop { get; set; }
    }
}
