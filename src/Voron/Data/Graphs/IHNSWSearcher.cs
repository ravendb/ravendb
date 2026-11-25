using System;
using Voron.Util;

namespace Voron.Data.Graphs;

public interface IHnswSearcher : IDisposable
{
    // Runs the search, once, and returns the indexes of the nodes it found, nearest first, and their distances to the query in the same
    // order. The lists belong to the searcher.
    public (ContextBoundNativeList<int> Results, ContextBoundNativeList<float> Distances) Search();

    // Returns the number of nodes processed by the searcher as candidates. It does not count nodes only queued for consideration.
    public long CandidatesProcessed { get; }
}
