using System.Runtime.CompilerServices;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public partial class SearchState
    {
        private struct EmptySearcher(int numberOfCandidates) : IHnswSearcher
        {
            public void Dispose()
            {
            }

            public bool MoveNextBatch() => false;

            public bool TryGetCurrentCandidates(out ContextBoundNativeList<int> candidates)
            {
                Unsafe.SkipInit(out candidates);
                return false;
            }

            public void IncreaseNumberOfCandidates(int currentlyAcceptedNodes)
            {
            }

            public long CandidatesProcessed
            {
                get => 0L;
                set => _ = value;
            }

            public bool ShouldContinueSearch(long filterDocsCount)
            {
                return false;
            }

            public int NumberOfCandidates { get; private set; } = numberOfCandidates;
        }
    }
}
