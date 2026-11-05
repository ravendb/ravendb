using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public partial class SearchState
    {
        private struct EmptySearcher : IHnswSearcher
        {
            public void Dispose()
            {
            }

            public ContextBoundNativeList<int> Search() => default;

            public long CandidatesProcessed => 0L;
        }
    }
}
