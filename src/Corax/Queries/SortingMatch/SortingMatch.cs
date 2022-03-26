using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using static Corax.Queries.SortingMatch;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct SortingMatch<TInner, TComparer> : IQueryMatch
        where TInner : IQueryMatch
        where TComparer : struct, IMatchComparer
    {
        private readonly IndexSearcher _searcher;
        private readonly IQueryMatch _inner;        
        private readonly TComparer _comparer;
        private readonly int _take;
        private readonly bool _isScoreComparer;

        public long TotalResults;

        public SortingMatch(IndexSearcher searcher, in TInner inner, in TComparer comparer, int take = -1)
        {
            _searcher = searcher;
            _inner = inner;
            _take = take;
            _comparer = comparer;
            _isScoreComparer = typeof(TComparer) == typeof(BoostingComparer);

            TotalResults = 0;
        }

        public long Count => throw new NotSupportedException();

        public QueryCountConfidence Confidence => throw new NotSupportedException();

        public bool IsBoosting => _inner.IsBoosting || _isScoreComparer;

        public int AndWith(Span<long> buffer, int matches)
        {
            throw new NotSupportedException($"{nameof(SortingMatch<TInner, TComparer>)} does not support the operation of {nameof(AndWith)}.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            if (typeof(TComparer) == typeof(BoostingComparer))
                return FillScore(matches);
            else
            {
                return _comparer.FieldType switch
                {
                    MatchCompareFieldType.Sequence => Fill<SequenceItem>(matches),
                    MatchCompareFieldType.Integer => Fill<NumericalItem<long>>(matches),
                    MatchCompareFieldType.Floating => Fill<NumericalItem<double>>(matches),
                    MatchCompareFieldType.Score => FillScore(matches),
                    _ => throw new ArgumentOutOfRangeException(_comparer.FieldType.ToString())
                };
            }
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
        private int FillScore(Span<long> matches)
        {
            // Important: If you are going to request a massive take like 20K you need to pass at least a 20K size buffer to work with.
            //            The rationale for such behavior is that sorting has to find among the candidates the order between elements,
            //            and it can't do so without checking every single element found. If you fail to do so, your results may not be
            //            correct. 
            Debug.Assert(_take <= matches.Length);

            int totalMatches = _inner.Fill(matches);
            if (totalMatches == 0)
                return 0;

            int take = _take <= 0 ? matches.Length : Math.Min(matches.Length, _take);

            int floatArraySize = 2 * sizeof(float) * matches.Length;
            int matchesArraySize = sizeof(long) * matches.Length;
            var bufferHolder = QueryContext.MatchesRawPool.Rent(floatArraySize + matchesArraySize);
            var allScoresValues = MemoryMarshal.Cast<byte, float>(bufferHolder.AsSpan().Slice(0, floatArraySize));

            // PERF: We want to avoid to share cache lines, that's why the second array will move toward the end of the array. 
            var matchesScores = allScoresValues[..matches.Length];
            var bScores = allScoresValues[^matches.Length..];           

            TotalResults += totalMatches;

            // TODO: Analyze if it makes sense to have an alternative version aimed at handling smalls sets where we gather
            //       all the matches and then do the score work instead of doing it batch by batch. 

            // Initializing the scores and retrieve them.
            matchesScores.Fill(1);  
            _inner.Score(matches[0..totalMatches], matchesScores[0..totalMatches]);

            // We sort the first batch
            var sorter = new Sorter<float, long, NumericDescendingComparer>();
            sorter.Sort(matchesScores[0..totalMatches], matches[0..totalMatches]);

            Span<long> bValues = MemoryMarshal.Cast<byte, long>(bufferHolder.AsSpan().Slice(floatArraySize, matchesArraySize));
            var searcher = _searcher;
            while (true)
            {
                // We get a new batch
                int bTotalMatches = _inner.Fill(bValues);
                TotalResults += bTotalMatches;

                // When we don't have any new batch, we are done.
                if (bTotalMatches == 0)
                    break;

                // Initialize the scores and retrieve scores from the new batch. 
                bScores.Fill(1);
                _inner.Score(bValues[0..bTotalMatches], bScores[0..bTotalMatches]);

                int bIdx = 0;
                int kIdx = 0;

                // Get rid of all the elements that are bigger than the last one.
                ref var lastElement = ref matchesScores[take - 1];
                for (; bIdx < bTotalMatches; bIdx++)
                {
                    if (lastElement >= bScores[bIdx])
                        bScores[kIdx++] = bScores[bIdx];
                }
                bTotalMatches = kIdx;

                // We sort the new batch
                sorter.Sort(bScores[0..bTotalMatches], bValues);

                // We merge both batches. 
                int aTotalMatches = Math.Min(totalMatches, take);

                int aIdx = aTotalMatches;
                bIdx = 0;
                while (aIdx > 0 && aIdx >= aTotalMatches / 8)
                {
                    // If the 'bigger' of what we had is 'bigger than'
                    if (matchesScores[aIdx - 1] <= bScores[0])
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
                    var result = matchesScores[aIdx] < bScores[bIdx];

                    if (result)
                    {
                        matches[kIdx] = matches[aIdx];
                        aIdx++;
                    }
                    else
                    {
                        matches[kIdx] = bValues[bIdx];
                        matchesScores[kIdx] = bScores[bIdx];
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
                    matches[kIdx++] = matches[aIdx++];
                }

                while (bIdx < bTotalMatches && kIdx < take)
                {
                    matches[kIdx] = bValues[bIdx];
                    matchesScores[kIdx] = bScores[bIdx]; // We are using a new key, therefore we have to update it. 
                    kIdx++;
                    bIdx++;
                }

                End:
                totalMatches = kIdx;
            }

            QueryContext.MatchesRawPool.Return(bufferHolder);
            return totalMatches;
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

            int totalMatches = _inner.Fill(matches);
            if (totalMatches == 0)
                return 0;
            
            int matchesArraySize = sizeof(long) * matches.Length;
            int itemArraySize = 2 * Unsafe.SizeOf<MatchComparer<TComparer, W>.Item>() * matches.Length;
            var bufferHolder = QueryContext.MatchesRawPool.Rent(itemArraySize + matchesArraySize);

            var itemKeys = MemoryMarshal.Cast<byte, MatchComparer<TComparer, W>.Item>(bufferHolder.AsSpan().Slice(0, itemArraySize));
            Debug.Assert(itemKeys.Length == 2 * matches.Length);

            // PERF: We want to avoid to share cache lines, that's why the second array will move toward the end of the array. 
            var matchesKeys = itemKeys.Slice(0, matches.Length);
            Debug.Assert(matchesKeys.Length == matches.Length);
            var bKeys = itemKeys.Slice(matches.Length, matches.Length);
            Debug.Assert(bKeys.Length == matches.Length);

            int take = _take <= 0 ? matches.Length : Math.Min(matches.Length, _take);

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

            Span<long> bValues = MemoryMarshal.Cast<byte, long>(bufferHolder.AsSpan().Slice(itemArraySize, matchesArraySize));
            Debug.Assert(bValues.Length == matches.Length);
            while (true)
            {
                // We get a new batch
                int bTotalMatches = _inner.Fill(bValues);
                TotalResults += bTotalMatches;

                // When we don't have any new batch, we are done.
                if (bTotalMatches == 0)
                {
                    QueryContext.MatchesRawPool.Return(bufferHolder);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores) 
        {
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(SortingMatch)} [{typeof(TComparer).Name}]",
                children: new List<QueryInspectionNode> { _inner.Inspect()},
                parameters: new Dictionary<string, string>()
                {
                        { nameof(IsBoosting), IsBoosting.ToString() },
                });
        }

        string DebugView => Inspect().ToString();
    }
}
