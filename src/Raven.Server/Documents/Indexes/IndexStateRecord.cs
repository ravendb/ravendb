using System.Collections.Immutable;
using Raven.Server.Documents.Indexes.Workers;
using Voron.Data.BTrees;

namespace Raven.Server.Documents.Indexes;

public record IndexStateRecord(
    ImmutableDictionary<string, string> PrefixesOfReduceOutputDocumentsToDelete,
    HandleReferencesBase.State Documents,
    HandleReferencesBase.State Tombstones,
    ImmutableDictionary<string, IndexStateRecord.CollectionEtags> Collections,
    ImmutableDictionary<string, ImmutableDictionary<string, Tree.ChunkDetails[]>> DirectoriesByName)
{
    public static IndexStateRecord Empty = new IndexStateRecord(
        ImmutableDictionary<string, string>.Empty,
        HandleReferencesBase.State.Empty, 
        HandleReferencesBase.State.Empty,
        ImmutableDictionary<string, CollectionEtags>.Empty,
        ImmutableDictionary<string, ImmutableDictionary<string, Tree.ChunkDetails[]>>.Empty);
    
    public sealed class ReferenceCollectionEtags
    {
        public long LastEtag;
        public long LastProcessedTombstoneEtag;
    }

    public sealed record CollectionEtags(
        long LastIndexedEtag,
        long LastProcessedTombstoneEtag,
        ReferenceCollectionEtags LastReferencedEtagsForCompareExchange,
        ImmutableDictionary<string, ReferenceCollectionEtags> LastReferencedEtags);
}
