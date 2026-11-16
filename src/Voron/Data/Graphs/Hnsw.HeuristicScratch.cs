using System;
using System.Buffers;
using Sparrow;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public partial class Registration
    {
        // The neighbor selection heuristic of the HNSW paper (https://arxiv.org/pdf/1603.09320, Algorithm 4): the nearest candidates
        // that no accepted candidate is nearer to, then the nearest pruned candidates up to the fill limit.
        // See: https://icode.best/i/45208840268843 - Chinese, but auto-translate works, and a good explanation with 
        // conjunction of: https://img-bc.icode.best/20210425010212938.png
                    
        // Its buffers come from the shared array pool, because it runs on the threads of the pool and the allocator of the transaction does not.
        private sealed class HeuristicScratch : IDisposable
        {
            private float[] _distances = [];
            private int[] _order = [];
            private int[] _pruned = [];

            // Writes the positions of the selected vectors to result, the accepted ones first, and returns how many there are.
            public int Select(SearchState searchState, UnmanagedSpan source, ReadOnlySpan<UnmanagedSpan> vectors, int maxEdges, int prunedFillLimit,
                Span<int> result)
            {
                EnsureLength(vectors.Length);
                var distances = _distances.AsSpan(0, vectors.Length);
                var order = _order.AsSpan(0, vectors.Length);
                for (int i = 0; i < vectors.Length; i++)
                {
                    distances[i] = searchState.Distance(source, vectors[i]);
                    order[i] = i;
                }

                distances.Sort(order);
                int accepted = 0;
                int pruned = 0;
                for (int i = 0; i < vectors.Length && accepted < maxEdges; i++)
                {
                    int candidate = order[i];
                    if (IsNearerToAnAccepted(searchState, vectors, candidate, distances[i], result[..accepted]))
                        _pruned[pruned++] = candidate;
                    else
                        result[accepted++] = candidate;
                }

                int count = accepted;
                for (int i = 0; i < pruned && count < prunedFillLimit; i++)
                    result[count++] = _pruned[i];
                return count;
            }

            private static bool IsNearerToAnAccepted(SearchState searchState, ReadOnlySpan<UnmanagedSpan> vectors, int candidate, float distanceToSource,
                ReadOnlySpan<int> accepted)
            {
                foreach (var selected in accepted)
                {
                    if (searchState.Distance(vectors[candidate], vectors[selected]) < distanceToSource)
                        return true;
                }

                return false;
            }

            private void EnsureLength(int length)
            {
                if (_distances.Length >= length)
                    return;

                Return();
                _distances = ArrayPool<float>.Shared.Rent(length);
                _order = ArrayPool<int>.Shared.Rent(length);
                _pruned = ArrayPool<int>.Shared.Rent(length);
            }

            private void Return()
            {
                if (_distances.Length == 0)
                    return;

                ArrayPool<float>.Shared.Return(_distances);
                ArrayPool<int>.Shared.Return(_order);
                ArrayPool<int>.Shared.Return(_pruned);
                _distances = [];
                _order = [];
                _pruned = [];
            }

            public void Dispose() => Return();
        }
    }
}
