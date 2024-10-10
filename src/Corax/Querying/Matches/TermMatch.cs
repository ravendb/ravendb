using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Util.PFor;

namespace Corax.Querying.Matches
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct TermMatch : IQueryMatch
    {
        private readonly delegate*<ref TermMatch, Span<long>, int> _fillFunc;
        private readonly delegate*<ref TermMatch, Span<long>, int, int> _andWithFunc;
        private readonly delegate*<ref TermMatch, Span<long>, Span<float>, float, void> _scoreFunc;
        private readonly delegate*<ref TermMatch, QueryInspectionNode> _inspectFunc;

        private bool _returnedValue;
        private readonly long _totalResults;
        private long _current;
        internal Bm25Relevance _bm25Relevance;
        private PostingList.Iterator _set;
        private Container.Item _containerItem;
        private FastPForBufferedReader _containerReader;
        private ByteStringContext _ctx;
        public bool IsBoosting => _scoreFunc != null;
        public long Count => _totalResults;

#if DEBUG
        public string Term;
#endif

        public SkipSortingResult AttemptToSkipSorting()
        {
            return SkipSortingResult.ResultsNativelySorted;
        }

        public QueryCountConfidence Confidence => QueryCountConfidence.High;

        private TermMatch(
            Querying.IndexSearcher indexSearcher,
            ByteStringContext ctx,
            long totalResults,
            delegate*<ref TermMatch, Span<long>, int> fillFunc,
            delegate*<ref TermMatch, Span<long>, int, int> andWithFunc,
            delegate*<ref TermMatch, Span<long>, Span<float>, float, void> scoreFunc = null,
            delegate*<ref TermMatch, QueryInspectionNode> inspectFunc = null)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _fillFunc = fillFunc;
            _andWithFunc = andWithFunc;
            _scoreFunc = scoreFunc;
            _inspectFunc = inspectFunc;
            _ctx = ctx;
            _containerItem = default;
            _set = default;
            _containerReader = default;
#if DEBUG
            Term = null;
#endif
        }

        public static TermMatch CreateEmpty(Querying.IndexSearcher indexSearcher, ByteStringContext ctx)
        {
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                term._current = QueryMatch.Invalid;
                return 0;
            }

            static int AndWithFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                term._current = QueryMatch.Invalid;
                return 0;
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Empty]",
                    parameters: new Dictionary<string, string>()
                    {
                        { Constants.QueryInspectionNode.IsBoosting, term.IsBoosting.ToString() },
                        { Constants.QueryInspectionNode.Count, term.Count.ToString() },
                        { Constants.QueryInspectionNode.CountConfidence, term.Confidence.ToString() },
                    });
            }

            return new TermMatch(indexSearcher, ctx, 0, &FillFunc, &AndWithFunc, inspectFunc: &InspectFunc)
            {
#if DEBUG
                Term = "<empty>"
#endif
            };
        }

        public static TermMatch YieldOnce(IndexSearcher indexSearcher, ByteStringContext ctx, long value, double termRatioToWholeCollection, bool isBoosting)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                if (term._returnedValue == false)
                {
                    term._returnedValue = true;
                    matches[0] = term._current;
                    return 1;
                }

                return 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                uint bot = 0;
                uint top = (uint)matches;

                long current = term._current;
                while (top > 1)
                {
                    uint mid = top / 2;

                    if (current >= Unsafe.Add(ref MemoryMarshal.GetReference(buffer), bot + mid))
                        bot += mid;
                    top -= mid;
                }

                if (current != Unsafe.Add(ref MemoryMarshal.GetReference(buffer), bot))
                    return 0;

                buffer[0] = current;
                return 1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void ScoreFunc(ref TermMatch term, Span<long> matches, Span<float> scores, float boostFactor)
            {
                using (term._bm25Relevance)
                    term._bm25Relevance.Score(matches, scores, boostFactor);
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Once]",
                    parameters: new Dictionary<string, string>()
                    {
                        { Constants.QueryInspectionNode.IsBoosting, term.IsBoosting.ToString() },
                        { Constants.QueryInspectionNode.Count, term.Count.ToString() },
                        { Constants.QueryInspectionNode.CountConfidence, term.Confidence.ToString() },
                    });
            }

            Bm25Relevance bm25Relevance = default;
            long current = -1;
            if (isBoosting)
            {
                bm25Relevance = Bm25Relevance.Once(indexSearcher, 1, ctx, 1, termRatioToWholeCollection);
                current = bm25Relevance.Add(value);
            }

            return new TermMatch(indexSearcher, ctx, 1, &FillFunc, &AndWithFunc, scoreFunc: isBoosting ? &ScoreFunc : null, inspectFunc: &InspectFunc)
            {
                _current = bm25Relevance is not null
                    ? current
                    : EntryIdEncodings.DecodeAndDiscardFrequency(value),
                _bm25Relevance = bm25Relevance,
                _returnedValue = false
            };
        }

        public static TermMatch YieldSmall(IndexSearcher indexSearcher, ByteStringContext ctx, Container.Item containerItem, double termRatioToWholeCollection,
            bool isBoosting)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc<TBoostingMode>(ref TermMatch term, Span<long> matches) where TBoostingMode : IBoostingMarker
            {
                int results;
                fixed (long* pMatches = matches)
                {
                    results = term._containerReader.Fill(pMatches, matches.Length);
                }

                if (results == 0)
                {
                    term._containerReader.Dispose();
                    return 0;
                }

                //Save the frequencies
                if (typeof(TBoostingMode) == typeof(HasBoosting))
                {
                    if (term._bm25Relevance.IsStored)
                        term._bm25Relevance.Process(matches, results);
                    else
                        EntryIdEncodings.DecodeAndDiscardFrequency(matches, results);
                }
                else
                {
                    EntryIdEncodings.DecodeAndDiscardFrequency(matches, results);
                }

                return results;
            }

            [SkipLocalsInit]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc<TBoostingMode>(ref TermMatch term, Span<long> buffer, int matches) where TBoostingMode : IBoostingMarker
            {
                // AndWith has to start from the start.
                _ = VariableSizeEncoding.Read<int>(term._containerItem.Address, out var offset); // discard count here
                using var reader = new FastPForDecoder(term._ctx);
                reader.Init(term._containerItem.Address + offset, term._containerItem.Length - offset);

                var decodedMatches = stackalloc long[1024];

                int bufferIndex = 0;
                int matchedIndex = 0;
                while (bufferIndex < matches)
                {
                    var read = reader.Read(decodedMatches, 1024);
                    if (read == 0)
                        break;

                    for (int decodedIndex = 0; decodedIndex < read && bufferIndex < matches; decodedIndex++)
                    {
                        long current = decodedMatches[decodedIndex];
                        long decodedEntryId = typeof(TBoostingMode) == typeof(HasBoosting)
                            ? term._bm25Relevance.Add(current)
                            : EntryIdEncodings.DecodeAndDiscardFrequency(current);

                        while (buffer[bufferIndex] < decodedEntryId)
                        {
                            bufferIndex++;
                            if (bufferIndex >= matches)
                            {
                                if (typeof(TBoostingMode) == typeof(HasBoosting))
                                {
                                    //no match, we should discard last item.
                                    term._bm25Relevance.Remove();
                                }


                                goto End;
                            }
                        }

                        // If there is a match we advance. 
                        if (buffer[bufferIndex] == decodedEntryId)
                        {
                            buffer[matchedIndex++] = decodedEntryId;
                            bufferIndex++;
                        }
                    }
                }

                reader.Dispose();
                End:
                return matchedIndex;
            }

            static void ScoreFunc(ref TermMatch term, Span<long> matches, Span<float> scores, float boostFactor)
            {
                using (term._bm25Relevance)
                    term._bm25Relevance.Score(matches, scores, boostFactor);
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [SmallSet]",
                    parameters: new Dictionary<string, string>()
                    {
                        { Constants.QueryInspectionNode.IsBoosting, term.IsBoosting.ToString() },
                        { Constants.QueryInspectionNode.Count, term.Count.ToString() },
                        { Constants.QueryInspectionNode.CountConfidence, term.Confidence.ToString() },
                    });
            }

            var itemsCount = VariableSizeEncoding.Read<int>(containerItem.Address, out var offset);
            var reader = new FastPForBufferedReader(ctx, containerItem.Address + offset, containerItem.Length - offset);
            return new TermMatch(indexSearcher, ctx, itemsCount, isBoosting ? &FillFunc<HasBoosting> : &FillFunc<NoBoosting>,
                isBoosting ? &AndWithFunc<HasBoosting> : &AndWithFunc<NoBoosting>, inspectFunc: &InspectFunc, scoreFunc: isBoosting ? &ScoreFunc : null)
            {
                _bm25Relevance = isBoosting
                    ? Bm25Relevance.Small(indexSearcher, itemsCount, ctx, itemsCount, termRatioToWholeCollection)
                    : default,
                _current = 0,
                _containerItem = containerItem,
                _containerReader = reader
            };
        }

        public static TermMatch YieldSet(IndexSearcher indexSearcher, ByteStringContext ctx, PostingList postingList, double termRatioToWholeCollection, bool isBoosting,
            bool useAccelerated = true)
        {
            [SkipLocalsInit]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc<TBoostingMode>(ref TermMatch term, Span<long> matches, int matchesCount) where TBoostingMode : IBoostingMarker
            {
                if (matchesCount == 0)
                    return 0;
                
                var it = term._set;
                // There is no element equal to or greater than `min(matches)-1`. No matches possible anyway.
                if (it.Seek(EntryIdEncodings.PrepareIdForSeekInPostingList(matches[0] - 1)) == false)
                    return 0;
                
                Span<long> postingListBuffer = stackalloc long[1024];
                ref var postingListStartPtr = ref MemoryMarshal.GetReference(postingListBuffer);
                
                ref var resultStartPtr = ref MemoryMarshal.GetReference(matches);
                var resultIdx = 0;
                
                ref var matchesStartPtr = ref MemoryMarshal.GetReference(matches);
                var matchesIdx = 0;
                var matchesMax = matches[matchesCount - 1];

                var maxValidValue = EntryIdEncodings.PrepareIdForPruneInPostingList(matchesMax + 1);
                while (matchesIdx < matchesCount)
                {
                    //posting list is empty
                    if (it.Fill(postingListBuffer, out var postingListCount, maxValidValue) == false || postingListCount == 0)
                        break;
                    
                    var postingListMinValue = EntryIdEncodings.DecodeAndDiscardFrequency(Unsafe.Add(ref postingListStartPtr, 0));
                    if (matchesMax < postingListMinValue)
                        continue;
                    
                    var postingListMaxValue = EntryIdEncodings.DecodeAndDiscardFrequency(Unsafe.Add(ref postingListStartPtr, postingListCount - 1));
                    var matchesMinValue = Unsafe.Add(ref matchesStartPtr, matchesIdx);
                    if (postingListMaxValue < matchesMinValue)
                        continue;

                    var postingListIdx = 0;
                    while (postingListIdx < postingListCount && matchesIdx < matchesCount)
                    {
                        var currentMatchesMin = Unsafe.Add(ref matchesStartPtr, matchesIdx);
                        var currentPostingListMin = EntryIdEncodings.DecodeAndDiscardFrequency(Unsafe.Add(ref postingListStartPtr, postingListIdx));
                        
                        if (typeof(TBoostingMode) == typeof(HasBoosting) && currentMatchesMin == currentPostingListMin)
                            term._bm25Relevance.Add(Unsafe.Add(ref postingListStartPtr, postingListIdx));
                        
                        Unsafe.Add(ref resultStartPtr, resultIdx) = currentMatchesMin;
                        resultIdx += (currentMatchesMin == currentPostingListMin).ToInt32();
                        postingListIdx += (currentMatchesMin >= currentPostingListMin).ToInt32();
                        matchesIdx += (currentMatchesMin <= currentPostingListMin).ToInt32();
                    }
                }
                
                return resultIdx;
            }

            [SkipLocalsInit]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithVectorizedFunc<TBoostingMode>(ref TermMatch term, Span<long> buffer, int matches) where TBoostingMode : IBoostingMarker
            {
                const int BlockSize = 4096;
                uint N = (uint)Vector256<long>.Count;

                Debug.Assert(Vector256<long>.Count == 4);

                term._set.Seek(EntryIdEncodings.PrepareIdForSeekInPostingList(buffer[0] - 1));

                // PERF: The AND operation can be performed in place, because we end up writing the same value that we already read. 
                fixed (long* inputStartPtr = buffer)
                {
                    long* inputEndPtr = inputStartPtr + matches;

                    // The size of this array is fixed to improve cache locality.
                    using var _ = term._ctx.Allocate(BlockSize * sizeof(long), out var bufferHolder);
                    var blockMatches = MemoryMarshal.Cast<byte, long>(bufferHolder.ToSpan());
                    Debug.Assert(blockMatches.Length == BlockSize);

                    long* blockStartPtr = (long*)bufferHolder.Ptr;

                    long* inputPtr = inputStartPtr;
                    long* dstPtr = inputStartPtr;
                    while (inputPtr < inputEndPtr)
                    {
                        var result = term._set.Fill(blockMatches, out int read,
                            pruneGreaterThanOptimization: EntryIdEncodings.PrepareIdForPruneInPostingList(buffer[matches - 1]));
                        if (result == false)
                            break;

                        if (typeof(TBoostingMode) == typeof(HasBoosting))
                            term._bm25Relevance.Process(blockMatches, read);
                        else
                            EntryIdEncodings.DecodeAndDiscardFrequency(blockMatches, read);

                        Debug.Assert(read <= BlockSize);

                        if (read == 0)
                            continue;

                        long* smallerPtr, largerPtr;
                        long* smallerEndPtr, largerEndPtr;

                        bool applyVectorization;

                        // See: MergeHelper.AndVectorized
                        // read => leftLength
                        // matches => rightLength
                        bool isSmallerInput;
                        if (read < (inputEndPtr - inputPtr))
                        {
                            smallerPtr = blockStartPtr;
                            smallerEndPtr = blockStartPtr + read;
                            isSmallerInput = false;
                            largerPtr = inputPtr;
                            largerEndPtr = inputEndPtr;
                            applyVectorization = matches > N && read > 0;
                        }
                        else
                        {
                            smallerPtr = inputPtr;
                            smallerEndPtr = inputEndPtr;
                            isSmallerInput = true;
                            largerPtr = blockStartPtr;
                            largerEndPtr = blockStartPtr + read;
                            applyVectorization = read > N && matches > 0;
                        }

                        Debug.Assert((ulong)(smallerEndPtr - smallerPtr) <= (ulong)(largerEndPtr - largerPtr));

                        if (applyVectorization)
                        {
                            while (true)
                            {
                                // TODO: In here we can do SIMD galloping with gather operations. Therefore we will be able to do
                                //       multiple checks at once and find the right amount of skipping using a table. 

                                // If the value to compare is bigger than the biggest element in the block, we advance the block. 
                                if ((ulong)*smallerPtr > (ulong)*(largerPtr + N - 1))
                                {
                                    if (largerPtr + N >= largerEndPtr)
                                        break;

                                    largerPtr += N;
                                    continue;
                                }

                                // If the value to compare is smaller than the smallest element in the block, we advance the scalar value.
                                if ((ulong)*smallerPtr < (ulong)*largerPtr)
                                {
                                    smallerPtr++;
                                    if (smallerPtr >= smallerEndPtr)
                                        break;

                                    continue;
                                }

                                if (largerEndPtr - largerPtr < N)
                                    break; // boundary guardian for vector load.

                                Vector256<ulong> value = Vector256.Create((ulong)*smallerPtr);
                                Vector256<ulong> blockValues = Vector256.Load((ulong*)largerPtr);

                                // We are going to select which direction we are going to be moving forward. 
                                if (Vector256.EqualsAny(value, blockValues))
                                {
                                    // We found the value, therefore we need to store this value in the destination.
                                    *dstPtr = *smallerPtr;
                                    dstPtr++;
                                }

                                smallerPtr++;
                                if (smallerPtr >= smallerEndPtr)
                                    break;
                            }
                        }

                        // The scalar version. This shouldn't cost much either way. 
                        while (smallerPtr < smallerEndPtr && largerPtr < largerEndPtr)
                        {
                            ulong leftValue = (ulong)*smallerPtr;
                            ulong rightValue = (ulong)*largerPtr;

                            if (leftValue > rightValue)
                            {
                                largerPtr++;
                            }
                            else if (leftValue < rightValue)
                            {
                                smallerPtr++;
                            }
                            else
                            {
                                *dstPtr = (long)leftValue;
                                dstPtr++;
                                smallerPtr++;
                                largerPtr++;
                            }
                        }

                        inputPtr = isSmallerInput ? smallerPtr : largerPtr;

                        // In AndWith operation the end buffer has to be exactly the same size as input or be smaller.
                        Debug.Assert(inputEndPtr >= dstPtr);
                        Debug.Assert((isSmallerInput ? largerPtr : smallerPtr) - blockStartPtr <= BlockSize);
                    }

                    return (int)((ulong*)dstPtr - (ulong*)inputStartPtr);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc<TBoostingMode>(ref TermMatch term, Span<long> matches) where TBoostingMode : IBoostingMarker
            {
                int i = 0;
                var set = term._set;

                set.Fill(matches, out i);

                if (typeof(TBoostingMode) == typeof(HasBoosting))
                {
                    if (term._bm25Relevance.IsStored == false)
                        EntryIdEncodings.DecodeAndDiscardFrequency(matches, i);
                    else
                        term._bm25Relevance.Process(matches, i);
                }
                else
                    EntryIdEncodings.DecodeAndDiscardFrequency(matches, i);

                term._set = set;
                return i;
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Set]",
                    parameters: new Dictionary<string, string>()
                    {
                        { Constants.QueryInspectionNode.IsBoosting, term.IsBoosting.ToString() },
                        { Constants.QueryInspectionNode.Count, term.Count.ToString() },
                        { Constants.QueryInspectionNode.CountConfidence, term.Confidence.ToString() },
                    });
            }

            static void ScoreFunc(ref TermMatch term, Span<long> matches, Span<float> scores, float boostFactor)
            {
                using (term._bm25Relevance)
                    term._bm25Relevance.Score(matches, scores, boostFactor);
            }

            if (Vector256.IsHardwareAccelerated == false)
                useAccelerated = false;

            var bm25Relevance = isBoosting
                ? Bm25Relevance.Set(indexSearcher, postingList.State.NumberOfEntries, ctx, (int)postingList.State.NumberOfEntries, termRatioToWholeCollection,
                    postingList)
                : default;

            var isStored = isBoosting && bm25Relevance.IsStored;

            // We will select the Vector256 version if supported.             
            return new TermMatch(indexSearcher, ctx, postingList.State.NumberOfEntries,
                (isBoosting, isStored) switch
                {
                    (isBoosting: true, isStored: true) => &FillFunc<HasBoosting>,
                    (isBoosting: true, isStored: false) => &FillFunc<HasBoostingNoStore>,
                    (_, _) => &FillFunc<NoBoosting>
                },
                (useAccelerated, isBoosting, isStored) switch
                {
                    (useAccelerated: true, isBoosting: true, isStored: true) => &AndWithVectorizedFunc<HasBoosting>,
                    (useAccelerated: true, isBoosting: true, isStored: false) => &AndWithVectorizedFunc<HasBoostingNoStore>,
                    (useAccelerated: true, isBoosting: false, isStored: _) => &AndWithVectorizedFunc<NoBoosting>,
                    (useAccelerated: false, isBoosting: true, isStored: false) => &AndWithFunc<HasBoostingNoStore>,
                    (useAccelerated: false, isBoosting: true, isStored: true) => &AndWithFunc<HasBoosting>,
                    (useAccelerated: false, isBoosting: false, isStored: _) => &AndWithFunc<NoBoosting>,
                },
                inspectFunc: &InspectFunc,
                scoreFunc: isBoosting ? &ScoreFunc : null) { _set = postingList.Iterate(), _current = long.MinValue, _bm25Relevance = bm25Relevance };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            return _fillFunc(ref this, matches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            return _andWithFunc(ref this, buffer, matches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            if (_scoreFunc == null)
            {
                return; // We ignore. Nothing to do here. 
            }

            _scoreFunc(ref this, matches, scores, boostFactor);
        }

        public QueryInspectionNode Inspect()
        {
            return _inspectFunc is null ? QueryInspectionNode.NotInitializedInspectionNode(nameof(TermMatch)) : _inspectFunc(ref this);
        }

        string DebugView => Inspect().ToString();
    }
}
