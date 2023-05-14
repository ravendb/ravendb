using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Newtonsoft.Json.Bson;
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
        public readonly int Max;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _matchesScope;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _termsScope;
        private int _capacity;

        public int Count;
        
        public string[] DebugEntries => DebugTerms(_llt, new(_terms, Count));

        public Results(LowLevelTransaction llt, ByteStringContext allocator, int max)
        {
            _llt = llt;
            _allocator = allocator;
            _capacity = Math.Max(128, max);

            Max = max == -1 ? int.MaxValue : max;
            
            Count = NotStarted;
            _matchesScope = allocator.Allocate(sizeof(long) * _capacity, out _matchesBuffer);
            _termsScope = allocator.Allocate(sizeof(UnmanagedSpan) * _capacity, out _termsBuffer);
            _matches = (long*)_matchesBuffer.Ptr;
            _terms = (UnmanagedSpan*)_termsBuffer.Ptr;
        }

        public void Append(long match)
        {
            _matches[Count++] = match;
        }
            
        public void Merge<TEntryComparer>(TEntryComparer comparer,
            Span<int> indexes,
            Span<long> matches, 
            UnmanagedSpan* terms)
            where TEntryComparer : struct, IComparer<UnmanagedSpan>
        {
            Debug.Assert(matches.Length == indexes.Length);

            if (Max == -1 || // no limit
                Count + matches.Length < Max) // *after* the batch, we are still below the limit
            {
                FullyMergeWith(comparer, indexes, matches, terms);
                return;
            }
                 
            if(Count < Max)
            {
                // fill up to maximum size
                int sizeToFull = Max - Count;
                FullyMergeWith(comparer, indexes[..sizeToFull], matches, terms);
                indexes = indexes[sizeToFull..];
            }
                
            if(indexes.Length > Max)
            {
                indexes = indexes[..Max];
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
                    (_matches[i], matches[bIdx]) = (matches[bIdx], _matches[i]);
                    (_terms[i], terms[bIdx]) = (terms[bIdx], _terms[i]);

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


        private void FullyMergeWith<TComparer>(TComparer comparer, Span<int> indexes, Span<long> matches, UnmanagedSpan* terms)
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

        private void CopyToHeadOfBuffer(Span<int> indexes, Span<long> matches, UnmanagedSpan* terms, int limit)
        {
            for (int i = 0; i < limit; i++)
            {
                var bIdx = indexes[i];
                _matches[i] = matches[bIdx];
                _terms[i] = terms[bIdx];
            }
        }

        public void EnsureAdditionalCapacity(int additionalCount)
        {
            if (Count + additionalCount < _capacity)
                return;
            EnsureCapacity(Count + additionalCount);
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
