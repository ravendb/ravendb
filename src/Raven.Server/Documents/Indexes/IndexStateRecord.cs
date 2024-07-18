using System;
using System.Collections.Immutable;
using System.Diagnostics;
using Corax;
using Lucene.Net.Search;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents.Indexes.Workers;
using Sparrow.Logging;
using Voron.Data.BTrees;

namespace Raven.Server.Documents.Indexes;

public record IndexStateRecord(
    ImmutableDictionary<string, string> PrefixesOfReduceOutputDocumentsToDelete,
    HandleReferencesBase.State Documents,
    HandleReferencesBase.State Tombstones,
    ImmutableDictionary<string, IndexStateRecord.CollectionEtags> Collections,
    ImmutableDictionary<string, ImmutableDictionary<string, Tree.ChunkDetails[]>> DirectoriesByName,
    LuceneIndexState LuceneIndexState,
    ImmutableDictionary<string, LuceneIndexState> LuceneSuggestionStates)
{

    public static IndexStateRecord Empty = new IndexStateRecord(
        ImmutableDictionary<string, string>.Empty,
        HandleReferencesBase.State.CreateEmpty(),
        HandleReferencesBase.State.CreateEmpty(),
        ImmutableDictionary<string, CollectionEtags>.Empty,
        ImmutableDictionary<string, ImmutableDictionary<string, Tree.ChunkDetails[]>>.Empty,
        new LuceneIndexState(),
        ImmutableDictionary<string, LuceneIndexState>.Empty);
    
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

public class LuceneIndexState
{
    public Lazy<IndexSearcher> IndexSearcher;

    public LuceneIndexState(Func<IndexSearcher> func)
    {
        IndexSearcher = new Lazy<IndexSearcher>(func);
    }
    public LuceneIndexState()
    {
        IndexSearcher = new Lazy<IndexSearcher>(() => throw new InvalidOperationException("The index searcher was not initialized"));
    }

    ~LuceneIndexState()
    {
        if (!IndexSearcher.IsValueCreated) 
            return;
        try
        {
            using(IndexSearcher.Value)
            using (IndexSearcher.Value.IndexReader)
            {
                
            }
        }
        catch (Exception e)
        {
            try
            {
                Logger logger = LoggingSource.Instance.GetLogger<IndexStateRecord>("Finalizer");
                if (logger.IsOperationsEnabled)
                {
                    logger.Operations("Failed to finalize index searcher", e);
                }
            }
            catch { }
            Debug.Assert(false, e.ToString());
        }
    }

    public void Recreate(Func<IndexSearcher> createIndexSearcher)
    {
        if (IndexSearcher.IsValueCreated == false)
            return;
        IndexSearcher = new Lazy<IndexSearcher>(createIndexSearcher);
    }
}
