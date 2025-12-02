using System.Collections.Generic;
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

            public IEnumerable<bool> Search()
            {
                yield break;
            }

            public bool TryGetCurrentCandidates(out ContextBoundNativeList<int> candidates)
            {
                Unsafe.SkipInit(out candidates);
                return false;
            }

            public void IncreaseNumberOfCandidates(int delta)
            {
            }

            public long CandidatesProcessed
            {
                get => 0L;
                set => _ = value;
            }

            public int NumberOfCandidates { get; private set; } = numberOfCandidates;
        }
    }
}
