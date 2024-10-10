using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax;
using Lucene.Net.Search;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;
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
        long LastProcessedTimeSeriesDeletedRangeEtag,
        ReferenceCollectionEtags LastReferencedEtagsForCompareExchange,
        ImmutableDictionary<string, ReferenceCollectionEtags> LastReferencedEtags);
}

public class LuceneIndexState
{
    private static readonly ConditionalWeakTable<IndexSearcher, IndexSearcherDisposer> InstancesThatGotRecreated = new();

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
            using (IndexSearcher.Value)
            using (IndexSearcher.Value.IndexReader)
            {

            }
        }
        catch (Exception e)
        {
            try
            {
                RavenLogger logger = RavenLogManager.Instance.GetLoggerForServer<IndexStateRecord>();
                if (logger.IsErrorEnabled)
                {
                    logger.Error("Failed to finalize index searcher", e);
                }
            }
            catch
            {
                // ignored
            }
            Debug.Assert(false, e.ToString());
        }
    }

    public void Recreate(Func<IndexSearcher> createIndexSearcher)
    {
        if (IndexSearcher.IsValueCreated == false)
            return;

        var old = IndexSearcher.Value;

        InstancesThatGotRecreated.Add(old, new IndexSearcherDisposer(old)); // this way we ensure the old searcher will get disposed - https://www.ayende.com/blog/199169-A/externalfinalizer-adding-a-finalizer-to-3rd-party-objects

        IndexSearcher = new Lazy<IndexSearcher>(createIndexSearcher);
    }

    private class IndexSearcherDisposer(IndexSearcher searcher)
    {
        ~IndexSearcherDisposer()
        {
            try
            {
                using (searcher)
                using (searcher.IndexReader)
                {

                }
            }
            catch (Exception e)
            {
                try
                {
                    RavenLogger logger = RavenLogManager.Instance.GetLoggerForServer<IndexStateRecord>();
                    if (logger.IsErrorEnabled)
                    {
                        logger.Error($"Failed to finalize index searcher from {nameof(IndexSearcherDisposer)}", e);
                    }
                }
                catch
                {
                    // ignored
                }
                Debug.Assert(false, e.ToString());
            }
            
        }
    }
}
