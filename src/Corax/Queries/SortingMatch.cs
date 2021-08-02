using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow;

namespace Corax.Queries
{
    public partial struct SortingMatch<TInner, TComparer> : IQueryMatch
        where TInner : IQueryMatch
        where TComparer : struct, IComparer<long>
    {
        private readonly IQueryMatch _inner;        
        private readonly TComparer _comparer;
        private readonly int _take;

        public SortingMatch(in TInner inner, in TComparer comparer, int take = -1)
        {
            _inner = inner;
            _take = take;
            _comparer = comparer;
        }

        public long Count => _inner.Count;

        public QueryCountConfidence Confidence => _inner.Confidence;

        public int AndWith(Span<long> prevMatches)
        {
            throw new NotSupportedException($"{nameof(SortingMatch<TInner, TComparer>)} does not support the operation of {nameof(AndWith)}.");
        }

        public int Fill(Span<long> matches)
        {
            int take = _take <= 0 ? matches.Length : _take;

            // We get the first batch. 
            int totalMatches = 0;
            var tmpMatches = matches;
            while (tmpMatches.Length != 0)
            {
                int read = _inner.Fill(tmpMatches);
                if (read == 0)
                    break;

                totalMatches += read;
                tmpMatches = matches.Slice(totalMatches);
            }

            // We sort the first batch
            var sorter = new Sorter<long, TComparer>(_comparer);
            sorter.Sort(matches.Slice(0, totalMatches));

            Span<long> a = stackalloc long[matches.Length];
            Span<long> b = stackalloc long[matches.Length];

            while (true)
            {
                // We get a new batch
                var tmp = b;
                int bTotalMatches = 0;
                while (tmp.Length != 0)
                {
                    int read = _inner.Fill(tmp);
                    if (read == 0)
                        break;

                    bTotalMatches += read;
                    tmp = b.Slice(bTotalMatches);
                }

                // When we don't have any new batch, we are done.
                if (bTotalMatches == 0)
                    return totalMatches;

                // If we have already a full set usable for the take. 
                int bIdx, kIdx;                
                if (totalMatches >= take)
                {
                    // PERF: Because we know the max value in the 'take' statement so we can actually get rid of a lot of data if there is inbalance.
                    //       For that we need a custom sorter that uses that information to do early prunning of results before sorting.
                    //       They key of performance is in being able to do that as much as possible. 

                    bIdx = 0;
                    kIdx = 0;
                    long lastElement = matches[take - 1];
                    while (bIdx < bTotalMatches)
                    {
                        if (_comparer.Compare(lastElement, b[bIdx]) >= 0)
                            b[kIdx++] = b[bIdx];
                        bIdx++;
                    }
                    bTotalMatches = kIdx;
                }

                // When we don't have any new potential match here, we are done.
                if (bTotalMatches == 0)
                    continue;

                // We sort the new batch
                sorter.Sort(b.Slice(0, bTotalMatches));

                // We merge both batches. 
                int aTotalMatches = Math.Min(totalMatches, take);

                int aIdx = aTotalMatches;
                bIdx = 0;
                kIdx = 0;

                while (aIdx > 0 && aIdx >= aTotalMatches / 8)
                {
                    // If the 'bigger' of what we had is 'bigger than'
                    if (_comparer.Compare(matches[aIdx-1], b[0]) <= 0)
                        break;

                    aIdx /= 2;
                }

                // This is the new start location on the matches. 
                kIdx = aIdx; 

                // If we bailed on the first check, nothing to do here. 
                if (aIdx == aTotalMatches - 1)
                    goto End;

                // We copy the current results into the a array.
                matches.CopyTo(a);

                // PERF: This can be improved with TimSort like techniques (Galloping) but given the amount of registers and method calls
                //       involved requires careful timing to understand if we are able to gain vs a more compact code and predictable
                //       memory access patterns. 

                while (aIdx < aTotalMatches && bIdx < bTotalMatches && kIdx < take)
                    matches[kIdx++] = _comparer.Compare(a[aIdx], b[bIdx]) < 0 ? a[aIdx++] : b[bIdx++];

                // If there is no more space in the buffer, discard everything else.
                if (kIdx >= take)
                    goto End;

                // PERF: We could improve this with a CopyTo (won't do that for now). 

                // Copy the rest, given that we have failed on one of the other 2 only a single one will execute.
                while (aIdx < aTotalMatches && kIdx < take)
                    matches[kIdx++] = a[aIdx++];

                while (bIdx < bTotalMatches && kIdx < take)
                    matches[kIdx++] = b[bIdx++];

                End:
                totalMatches = kIdx;
            }
        }
    }
}
