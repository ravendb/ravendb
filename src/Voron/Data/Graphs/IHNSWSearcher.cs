using System;
using Voron.Util;

namespace Voron.Data.Graphs;

public interface IHnswSearcher : IDisposable
{
    // Runs the search, once, and returns the indexes of the nodes it found, nearest first. The list belongs to the searcher.
    public ContextBoundNativeList<int> Search();

    // Returns the number of nodes processed by the searcher as candidates. It does not count nodes only queued for consideration.
    public long CandidatesProcessed { get; }
}
