using System;
using Voron.Util;

namespace Voron.Data.Graphs;

public interface IHnswSearcher : IDisposable
{
    // Advances the search until the next batch of (previously unseen) candidates is ready for draining via
    // TryGetCurrentCandidates. Returns true while batches remain, false once the search is exhausted.
    public bool MoveNextBatch();

    // Retrieve IDs of nodes from the current search batch.
    // Guarantee: always returns previously unseen nodes.
    // If no new nodes are found, returns false.
    public bool TryGetCurrentCandidates(out ContextBoundNativeList<int> candidates);

    public void IncreaseNumberOfCandidates(int currentlyAcceptedNodes);

    // Returns the number of nodes processed by the searcher as candidates. It does not count nodes only queued for consideration.
    public long CandidatesProcessed { get; }

    // Number of vector distance computations (SimilarityCalc calls) performed. Excludes nodes skipped before the
    // distance is taken (e.g. tombstones) and revisits served from the per-query distance cache.
    public long VectorComparisons { get; }

    public bool ShouldContinueSearch(long filterDocsCount);
    
    // Number of candidates requested by the caller.
    public int NumberOfCandidates { get; }
}
