using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;

namespace Corax.Queries
{
    public unsafe struct SortingMatch<TInner, TComparer> : IQueryMatch
        where TInner : IQueryMatch
        where TComparer : struct, IMatchComparer
    {
        private readonly IndexSearcher _searcher;
        private readonly IQueryMatch _inner;        
        private readonly TComparer _comparer;
        private readonly int _take;
        public long TotalResults;
        private struct SequenceItem
        {
            public readonly byte* Ptr;
            public readonly int Size;

            public SequenceItem( byte* ptr, int size)
            {
                Ptr = ptr;
                Size = size;
            }
        }

        private struct NumericalItem<T> where T : unmanaged
        {
            public readonly T Value;
            public NumericalItem(in T value)
            {
                Value = value;
            }
        }

        private struct MatchComparer<T, W> : IComparer<MatchComparer<T, W>.Item>
            where T : IMatchComparer
            where W : struct
        {
            public struct Item
            {
                public long Key;
                public W Value;
            }

            private readonly T _comparer;

            public MatchComparer(in T comparer)
            {
                _comparer = comparer;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(Item ix, Item iy)
            {
                if (ix.Key > 0 && iy.Key > 0)
                {
                    if (typeof(W) == typeof(SequenceItem))
                    {
                        return _comparer.CompareSequence(
                            new ReadOnlySpan<byte>(((SequenceItem)(object)ix.Value).Ptr, ((SequenceItem)(object)ix.Value).Size),
                            new ReadOnlySpan<byte>(((SequenceItem)(object)iy.Value).Ptr, ((SequenceItem)(object)iy.Value).Size));
                    }
                    else if (typeof(W) == typeof(NumericalItem<long>))
                    {
                        return _comparer.CompareNumerical(((NumericalItem<long>)(object)ix.Value).Value, ((NumericalItem<long>)(object)iy.Value).Value);
                    }
                    else if (typeof(W) == typeof(NumericalItem<double>))
                    {
                        return _comparer.CompareNumerical(((NumericalItem<double>)(object)ix.Value).Value, ((NumericalItem<double>)(object)iy.Value).Value);
                    }
                }
                else if (ix.Key > 0)
                {
                    return 1;
                }

                return -1;
            }
        }

        public SortingMatch(IndexSearcher searcher, in TInner inner, in TComparer comparer, int take = -1)
        {
            _searcher = searcher;
            _inner = inner;
            _take = take;
            _comparer = comparer;
            TotalResults = 0;
        }

        public long Count => throw new NotSupportedException();

        public QueryCountConfidence Confidence => throw new NotSupportedException();

        public int AndWith(Span<long> prevMatches)
        {
            throw new NotSupportedException($"{nameof(SortingMatch<TInner, TComparer>)} does not support the operation of {nameof(AndWith)}.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            return _comparer.FieldType switch
            {
                MatchCompareFieldType.Sequence => Fill<SequenceItem>(matches),
                MatchCompareFieldType.Integer => Fill<NumericalItem<long>>(matches),
                MatchCompareFieldType.Floating => Fill<NumericalItem<double>>(matches),
                _ => throw new ArgumentOutOfRangeException(_comparer.FieldType.ToString())
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool Get<W>(IndexSearcher searcher, int fieldId, long x, out W key) where W : struct
        {
            var reader = searcher.GetReaderFor(x);
            if (typeof(W) == typeof(SequenceItem))
            {
                var readX = reader.Read(fieldId, out var sv);
                key = (W)(object)new SequenceItem((byte*)Unsafe.AsPointer(ref sv[0]), sv.Length);
                return readX;
            }
            else if (typeof(W) == typeof(NumericalItem<long>))
            {
                var readX = reader.Read<long>(fieldId, out var value);
                key = (W)(object)new NumericalItem<long>(value);
                return readX;
            }
            else if (typeof(W) == typeof(NumericalItem<double>))
            {
                var readX = reader.Read<double>(fieldId, out var value);
                key = (W)(object)new NumericalItem<double>(value);
                return readX;
            }

            Unsafe.SkipInit(out key);
            return false;
        }        

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Fill<W>(Span<long> matches) where W : struct
        {             
            // Important: If you are going to request a massive take like 20K you need to pass at least a 20K size buffer to work with.
            //            The rationale for such behavior is that sorting has to find among the candidates the order between elements,
            //            and it can't do so without checking every single element found. If you fail to do so, your results may not be
            //            correct. 
            Debug.Assert(_take <= matches.Length);

            var matchesKeysHolder = QueryContext.MatchesPool.Rent(2 * Unsafe.SizeOf<MatchComparer<TComparer, W>.Item>() * matches.Length);
            var itemKeys = MemoryMarshal.Cast<byte, MatchComparer<TComparer, W>.Item>(matchesKeysHolder);

            // PERF: We want to avoid to share cache lines, that's why the second array will move toward the end of the array. 
            var matchesKeys = itemKeys[0..matches.Length];            
            var bKeys = itemKeys[^matches.Length..]; 

            int take = _take <= 0 ? matches.Length : Math.Min(matches.Length, _take);

            int totalMatches = _inner.Fill(matches);
            if (totalMatches == 0)
                return 0;

            TotalResults += totalMatches;

            var searcher = _searcher;
            var fieldId = _comparer.FieldId;
            var comparer = new MatchComparer<TComparer, W>(_comparer);
            for (int i = 0; i < totalMatches; i++)
            {
                var read = Get(searcher, fieldId, matches[i], out matchesKeys[i].Value);
                matchesKeys[i].Key = read ? matches[i] : -matches[i];
            }

            // We sort the first batch
            var sorter = new Sorter<MatchComparer<TComparer, W>.Item, long, MatchComparer<TComparer, W>>(comparer);
            sorter.Sort(matchesKeys[0..totalMatches], matches);

            Span<long> bValues = stackalloc long[matches.Length];                        
            while (true)
            {
                // We get a new batch
                int bTotalMatches = _inner.Fill(bValues);
                TotalResults += bTotalMatches;

                // When we don't have any new batch, we are done.
                if (bTotalMatches == 0)
                {
                    QueryContext.MatchesPool.Return(matchesKeysHolder);
                    return totalMatches;
                }

                // We get the keys to sort.
                for (int i = 0; i < bTotalMatches; i++)
                {
                    var read = Get(searcher, fieldId, bValues[i], out bKeys[i].Value);
                    bKeys[i].Key = read ? bValues[i] : -bValues[i];
                }

                int bIdx = 0;
                int kIdx = 0;

                // Get rid of all the elements that are bigger than the last one.
                ref var lastElement = ref matchesKeys[take - 1];
                for (; bIdx < bTotalMatches; bIdx++)
                {
                    if (comparer.Compare(lastElement, bKeys[bIdx]) >= 0)
                        bKeys[kIdx++] = bKeys[bIdx];
                }
                bTotalMatches = kIdx;

                // We sort the new batch
                sorter.Sort(bKeys[0..bTotalMatches], bValues);                
                
                // We merge both batches. 
                int aTotalMatches = Math.Min(totalMatches, take);

                int aIdx = aTotalMatches;
                bIdx = 0;
                kIdx = 0;

                while (aIdx > 0 && aIdx >= aTotalMatches / 8)
                {
                    // If the 'bigger' of what we had is 'bigger than'
                    if (comparer.Compare(matchesKeys[aIdx-1], bKeys[0]) <= 0)
                        break;

                    aIdx /= 2;
                }

                // This is the new start location on the matches. 
                kIdx = aIdx; 

                // If we bailed on the first check, nothing to do here. 
                if (aIdx == aTotalMatches - 1 || kIdx >= take)
                    goto End;

                // PERF: This can be improved with TimSort like techniques (Galloping) but given the amount of registers and method calls
                //       involved requires careful timing to understand if we are able to gain vs a more compact code and predictable
                //       memory access patterns. 

                while (aIdx < aTotalMatches && bIdx < bTotalMatches && kIdx < take)
                {
                    var result = comparer.Compare(matchesKeys[aIdx], bKeys[bIdx]) < 0;

                    if (result)
                    {
                        matches[kIdx] = matchesKeys[aIdx].Key;
                        aIdx++;
                    }
                    else
                    {
                        matches[kIdx] = bKeys[bIdx].Key;
                        matchesKeys[kIdx] = bKeys[bIdx];
                        bIdx++;
                    }
                    kIdx++;
                }
                    
                // If there is no more space in the buffer, discard everything else.
                if (kIdx >= take)
                    goto End;

                // PERF: We could improve this with a CopyTo (won't do that for now). 

                // Copy the rest, given that we have failed on one of the other 2 only a single one will execute.
                while (aIdx < aTotalMatches && kIdx < take)
                {
                    matches[kIdx++] = matchesKeys[aIdx++].Key;
                }
                    
                while (bIdx < bTotalMatches && kIdx < take)
                {
                    matches[kIdx] = bKeys[bIdx].Key;
                    matchesKeys[kIdx] = bKeys[bIdx]; // We are using a new key, therefore we have to update it. 
                    kIdx++;
                    bIdx++;
                }
                    
                End:
                totalMatches = kIdx;
            }
        }
    }
}
