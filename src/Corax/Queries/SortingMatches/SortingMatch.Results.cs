using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Voron.Impl;

namespace Corax.Queries.SortingMatches;

unsafe partial struct SortingMatch<TInner> 
{
    private struct Results : IDisposable
    {
        private long* _matches;
        private UnmanagedSpan* _terms;
        private ByteString _matchesBuffer;
        private ByteString _termsBuffer;
        private readonly LowLevelTransaction _llt;
        private readonly ByteStringContext _allocator;
        private readonly int _max;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _matchesScope;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _termsScope;
        private int _capacity;

        public int Count;
        
        public string[] DebugEntries => DebugTerms(_llt, new(_terms, Count));

        public Results(LowLevelTransaction llt, ByteStringContext allocator, int max)
        {
            _llt = llt;
            _allocator = allocator;
            _max = max;
            Count = NotStarted;

            _capacity = Math.Max(128, max);
            _matchesScope = allocator.Allocate(sizeof(long) * _capacity, out _matchesBuffer);
            _termsScope = allocator.Allocate(sizeof(UnmanagedSpan) * _capacity, out _termsBuffer);
            _matches = (long*)_matchesBuffer.Ptr;
            _terms = (UnmanagedSpan*)_termsBuffer.Ptr;
        }
            
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static int BinarySearch<TEntryComparer>(TEntryComparer comparer,Span<int> indexes, Span<UnmanagedSpan> terms, UnmanagedSpan needle)
            where TEntryComparer : struct, IComparer<UnmanagedSpan>
        {
            int l = 0;
            int r = indexes.Length - 1;
            while (l <= r)
            {
                var pivot = (l + r) >> 1;
                var cmp = comparer.Compare(terms[indexes[pivot]], needle);
                switch (cmp)
                {
                    case 0:
                        return pivot;
                    case < 0:
                        l = pivot + 1;
                        break;
                    default:
                        r = pivot - 1;
                        break;
                }
            }

            return ~l;
        }

        public void Merge<TEntryComparer>(TEntryComparer comparer,
            Span<int> indexes,
            Span<long> matches, 
            Span<UnmanagedSpan> terms)
            where TEntryComparer : struct, IComparer<UnmanagedSpan>
        {
            Debug.Assert(matches.Length == terms.Length);
            Debug.Assert(matches.Length == indexes.Length);

            if (_max == -1 || // no limit
                Count + matches.Length < _max) // *after* the batch, we are still below the limit
            {
                FullyMergeWith(comparer, indexes, matches, terms);
                return;
            }
                 
            if(Count < _max)
            {
                // fill up to maximum size
                int sizeToFull = _max - Count;
                FullyMergeWith(comparer, indexes[..sizeToFull], matches, terms);
                indexes = indexes[sizeToFull..];
            }
                
            if(indexes.Length > _max)
            {
                indexes = indexes[.._max];
            }

            if (indexes.Length == 0)
                return;

            if (comparer.Compare(_terms[Count - 1], terms[indexes[0]]) < 0)
            {
                // all the items in this batch are *larger* than the smallest item
                // we keep, can skip it all
                return;
            }

            if (comparer.Compare(_terms[0], terms[indexes[^1]]) > 0)
            {
                // the first element we have is *larger* than the smallest
                // element in the next batch, just copy it over and done
                CopyToHeadOfBuffer(indexes, matches, terms, indexes.Length);
                return;
            }

            for (int i = 0; i < Count; i++)
            {
                var bIdx = indexes[0];
                var cmp = comparer.Compare(_terms[i], terms[bIdx]);
                if (cmp > 0)
                {
                    Swap(_matches, i, matches, bIdx);
                    Swap(_terms, i, terms, bIdx);

                    // put it in the right place
                    int j = 1;
                    for (; j < indexes.Length; j++)
                    {
                        cmp = comparer.Compare(terms[indexes[j]], terms[bIdx]);
                        if (cmp >= 0)
                            break;

                        indexes[j - 1] = indexes[j];
                    }

                    indexes[j-1] = bIdx;
                }
            }
        }

        private static void Swap<T>(T* a, int aIdx, Span<T> b, int bIdx) where T : unmanaged
        {
            (a[aIdx], b[bIdx]) = (b[bIdx], a[aIdx]);
        }
            


        private void FullyMergeWith<TComparer>(TComparer comparer, Span<int> indexes, Span<long> matches, Span<UnmanagedSpan> terms)
            where TComparer : struct, IComparer<UnmanagedSpan>
        {
            if (indexes.Length + Count > _capacity)
            {
                EnsureCapacity(indexes.Length + Count);
            }

            var b = indexes.Length - 1;
            var d = Count - 1;
            var k = indexes.Length + Count - 1;
            while (d >= 0 && b >= 0)
            {
                int bIdx = indexes[b];
                int cmp = comparer.Compare(_terms[d], terms[bIdx]);
                if (cmp < 0)
                {
                    _terms[k] = terms[bIdx];
                    _matches[k] = matches[bIdx];
                    b--;
                }
                else
                {
                    _terms[k] = _terms[d];
                    _matches[k] = _matches[d];
                    d--;
                }
                  
                k--;
            }

            CopyToHeadOfBuffer(indexes, matches, terms, b+1);
            Count += indexes.Length;
        }

        private void CopyToHeadOfBuffer(Span<int> indexes, Span<long> matches, Span<UnmanagedSpan> terms, int limit)
        {
            for (int i = 0; i < limit; i++)
            {
                var bIdx = indexes[i];
                _matches[i] = matches[bIdx];
                _terms[i] = terms[bIdx];
            }
        }

        private void EnsureCapacity(int req)
        {
            Debug.Assert(req > _capacity);

            var oldCapacity = _capacity;
            _capacity = (int)BitOperations.RoundUpToPowerOf2((uint)req);
            var additionalSize = _capacity - oldCapacity;
            _allocator.GrowAllocation(ref _matchesBuffer, ref _matchesScope, additionalSize * sizeof(long));
            _allocator.GrowAllocation(ref _termsBuffer, ref _termsScope, additionalSize * sizeof(UnmanagedSpan));
            _matches = (long*)_matchesBuffer.Ptr;
            _terms = (UnmanagedSpan*)_termsBuffer.Ptr;
        }

        public void Dispose()
        {
            _termsScope.Dispose();
            _matchesScope.Dispose();
        }

        public int CopyTo(Span<long> matches)
        {
            var copy = Math.Min(matches.Length, Count);
            new Span<long>(_matches, copy).CopyTo(matches);
            _matches += copy;
            Count -= copy;
            return copy;
        }

        internal void Init()
        {
            Count = 0;
        }
    }

}
