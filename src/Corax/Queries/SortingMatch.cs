using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;

namespace Corax.Queries
{
    public unsafe partial struct SortingMatch<TInner, TComparer> : IQueryMatch
        where TInner : IQueryMatch
        where TComparer : struct, IMatchComparer
    {
        private readonly IndexSearcher _searcher;
        private readonly IQueryMatch _inner;        
        private readonly TComparer _comparer;
        private readonly int _take;
        private unsafe struct SequenceItem
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

        private struct HashCacheMatchComparer<T, W> : IComparer<long>, IDisposable
            where W : struct
            where T : IMatchComparer
        {
            private struct Item
            {
                public long Match;
                public W Value;
            }

            private readonly IndexSearcher _searcher;
            private readonly T _comparer;
            private readonly int _fieldId;
            private Item[] _hash;

            private const ulong NoMatch = unchecked(1UL << 63);
            private const ulong MatchMask = unchecked((1UL << 63) - 1);
            private const int CacheSize = 4096;

            public HashCacheMatchComparer(IndexSearcher searcher, in T comparer)
            {
                _searcher = searcher;
                _comparer = comparer;
                _fieldId = comparer.FieldId;

                var hash = ArrayPool<Item>.Shared.Rent(CacheSize);
                for (int i = 0; i < hash.Length; i++)
                    hash[i].Match = 0;
                _hash = hash;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(long x, long y)
            {
                // Map into the hash space
                uint hx = ((uint)Hashing.Mix(x)) % CacheSize;
                uint hy = ((uint)Hashing.Mix(y)) % CacheSize;

                // Retrieve the potential element
                ref var ix = ref _hash[hx];
                ref var iy = ref _hash[hy];

                bool readX;
                if (ix.Match == ((long)((ulong)x & MatchMask)))
                {
                    // Found it, we gonna use it.
                    readX = ((ulong)ix.Match & NoMatch) == 0;
                }
                else
                {
                    var reader = _searcher.GetReaderFor(x);
                    if (typeof(W) == typeof(SequenceItem))
                    {
                        readX = reader.Read(_fieldId, out var sv);
                        ix.Value = (W)(object)new SequenceItem((byte*)Unsafe.AsPointer(ref sv[0]), sv.Length);
                    }
                    else if (typeof(W) == typeof(NumericalItem<long>))
                    {
                        readX = reader.Read<long>(_fieldId, out var value);
                        ix.Value = (W)(object)new NumericalItem<long>(value);
                    }
                    else if (typeof(W) == typeof(NumericalItem<double>))
                    {
                        readX = reader.Read<double>(_fieldId, out var value);
                        ix.Value = (W)(object)new NumericalItem<double>(value);
                    }
                    else return ThrowNotSupportedException();

                    ix.Match = (long)((ulong)x | (readX ? 0 : NoMatch));
                }

                bool readY;
                if (iy.Match == ((long)((ulong)y & MatchMask)))
                {
                    // Found it, we gonna use it.
                    readY = ((ulong)iy.Match & NoMatch) == 0;
                }
                else
                {
                    // Load the value;
                    var reader = _searcher.GetReaderFor(y);
                    if (typeof(W) == typeof(SequenceItem))
                    {
                        readY = reader.Read(_fieldId, out var sv);
                        iy.Value = (W)(object)new SequenceItem((byte*)Unsafe.AsPointer(ref sv[0]), sv.Length);
                    }
                    else if (typeof(W) == typeof(NumericalItem<long>))
                    {
                        readY = reader.Read<long>(_fieldId, out var value);
                        iy.Value = (W)(object)new NumericalItem<long>(value);
                    }
                    else if (typeof(W) == typeof(NumericalItem<double>))
                    {
                        readY = reader.Read<double>(_fieldId, out var value);
                        iy.Value = (W)(object)new NumericalItem<double>(value);
                    }
                    else return ThrowNotSupportedException();

                    iy.Match = (long)((ulong)y | (readY ? 0 : NoMatch));
                }

                if (readX && readY)
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
                else if (readX)
                {
                    return 1;
                }

                return -1;
            }

            private int ThrowNotSupportedException()
            {
                throw new NotSupportedException("Not supported.");
            }

            public void Dispose()
            {
                ArrayPool<Item>.Shared.Return(_hash);
            }
        }



        public SortingMatch(IndexSearcher searcher, in TInner inner, in TComparer comparer, int take = -1)
        {
            _searcher = searcher;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
{
            if (_comparer.FieldType == MatchCompareFieldType.Sequence)
                return Fill<SequenceItem>(matches);
            else if (_comparer.FieldType == MatchCompareFieldType.Integer)
                return Fill<NumericalItem<long>>(matches);
            else
                return Fill<NumericalItem<double>>(matches);
        }

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Fill<W>(Span<long> matches) where W : struct
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
            using var hashCacheComparer = new HashCacheMatchComparer<TComparer, W>(_searcher, _comparer);
            var sorter = new Sorter<long, HashCacheMatchComparer<TComparer, W>>(hashCacheComparer);
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
                        if (hashCacheComparer.Compare(lastElement, b[bIdx]) >= 0)
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
                    if (hashCacheComparer.Compare(matches[aIdx-1], b[0]) <= 0)
                        break;

                    aIdx /= 2;
                }

                // This is the new start location on the matches. 
                kIdx = aIdx; 

                // If we bailed on the first check, nothing to do here. 
                if (aIdx == aTotalMatches - 1 || kIdx >= take)
                    goto End;

                // We copy the current results into the a array.
                matches.CopyTo(a);

                // PERF: This can be improved with TimSort like techniques (Galloping) but given the amount of registers and method calls
                //       involved requires careful timing to understand if we are able to gain vs a more compact code and predictable
                //       memory access patterns. 

                while (aIdx < aTotalMatches && bIdx < bTotalMatches && kIdx < take)
                    matches[kIdx++] = hashCacheComparer.Compare(a[aIdx], b[bIdx]) < 0 ? a[aIdx++] : b[bIdx++];

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
