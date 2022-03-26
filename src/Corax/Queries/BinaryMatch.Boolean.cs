using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    unsafe partial struct BinaryMatch<TInner, TOuter>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BinaryMatch<TInner, TOuter> YieldAnd(in TInner inner, in TOuter outer)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref BinaryMatch<TInner, TOuter> match, Span<long> matches)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                while (true)
                {
                    int totalResults = 0;
                    int iterations = 0;

                    // PERF: An alternative implementation would be to perform OR in place. The upside is that every improvement on
                    //       OR would impact everywhere this happens, but the vectorized Sort may also tip the balance here. Another
                    //       good behavior would be that in cases of many duplicates we will have a better use of the buffer because
                    //       OR will also deduplicate on every call. 

                    var resultsSpan = matches;
                    while (resultsSpan.Length > 0)
                    {
                        // RavenDB-17750: We have to fill everything possible UNTIL there are no more matches availables.
                        var results = inner.Fill(resultsSpan);
                        if (results == 0)
                            break;

                        totalResults += results;
                        iterations++;

                        resultsSpan = resultsSpan.Slice(results);
                    }

                    // The problem is that multiple Fill calls do not ensure that we will get a sequence of ordered
                    // values, therefore we must ensure that we get a 'sorted' sequence ensuring those happen.
                    if (iterations > 1)
                    {
                        // We need to sort and remove duplicates.
                        var bufferBasePtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(matches));

                        MemoryExtensions.Sort(matches.Slice(0, totalResults));

                        // We need to fill in the gaps left by removing deduplication process.
                        // If there are no duplicated the writes at the architecture level will execute
                        // way faster than if there are.

                        var outputBufferPtr = bufferBasePtr;

                        var bufferPtr = bufferBasePtr;
                        var bufferEndPtr = bufferBasePtr + totalResults - 1;
                        while (bufferPtr < bufferEndPtr)
                        {
                            outputBufferPtr += bufferPtr[1] != bufferPtr[0] ? 1 : 0;
                            *outputBufferPtr = bufferPtr[1];

                            bufferPtr++;
                        }

                        totalResults = (int)(outputBufferPtr - bufferBasePtr + 1);
                    }

                    if (totalResults == 0)
                        return 0;

                    totalResults = outer.AndWith(matches, totalResults);
                    if (totalResults != 0)
                        return totalResults;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWith(ref BinaryMatch<TInner, TOuter> match, Span<long> buffer, int matches)
            {
                ref var inner = ref match._inner;
                var results = inner.AndWith(buffer, matches);
                if (results == 0)
                    return 0;

                ref var outer = ref match._outer;
                return outer.AndWith(buffer, results);
            }

            static QueryInspectionNode InspectFunc(ref BinaryMatch<TInner, TOuter> match)
            {
                return new QueryInspectionNode($"{nameof(BinaryMatch)} [And]",
                    children: new List<QueryInspectionNode> { match._inner.Inspect(), match._outer.Inspect() },
                    parameters: new Dictionary<string, string>()
                    {
                        { nameof(match.IsBoosting), match.IsBoosting.ToString() },
                        { nameof(match.Count), $"{match.Count} [{match.Confidence}]" }
                    });
            }

            // Estimate Confidence values.
            QueryCountConfidence confidence;
            if (inner.Count < outer.Count / 2)
                confidence = inner.Confidence;
            else if (outer.Count < inner.Count / 2)
                confidence = outer.Confidence;
            else
                confidence = inner.Confidence.Min(outer.Confidence);

            return new BinaryMatch<TInner, TOuter>(in inner, in outer, &FillFunc, &AndWith, &InspectFunc, Math.Min(inner.Count, outer.Count), confidence);
        }

        public static BinaryMatch<TInner, TOuter> YieldOr(in TInner inner, in TOuter outer)
        {
            [SkipLocalsInit]
            static int AndWith(ref BinaryMatch<TInner, TOuter> match, Span<long> buffer, int matches)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                var bufferHolder = QueryContext.MatchesRawPool.Rent(2 * sizeof(long) * matches);
                var innerBuffer = MemoryMarshal.Cast<byte, long>(bufferHolder);

                var innerMatches = innerBuffer.Slice(0, matches);
                var outerMatches = innerBuffer.Slice(matches, matches);
                Debug.Assert(innerMatches.Length == matches);
                Debug.Assert(outerMatches.Length == matches);

                // Important that we only keep the actual matches. 
                var actualMatches = buffer.Slice(0, matches);

                // Execute the AndWith operation for each subpart of the query.               
                actualMatches.CopyTo(innerMatches);
                int innerSize = inner.AndWith(innerMatches, matches);

                actualMatches.CopyTo(outerMatches);
                int outerSize = outer.AndWith(outerMatches, matches);
                
                // Merge the hits from every side into the output buffer. 
                var result = MergeHelper.Or(buffer, innerMatches.Slice(0, innerSize), outerMatches.Slice(0, outerSize));                
                QueryContext.MatchesRawPool.Return(bufferHolder);

                return result;
            }

            [SkipLocalsInit]
            static int FillFunc(ref BinaryMatch<TInner, TOuter> match, Span<long> matches)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                if (matches.Length == 0)
                    return 0;

                if (matches.Length == 1)
                {
                    // Special case when matches is a single element (no OR is possible under this conditions)
                    // PERF: For performance reason if this branch is been triggered repeteadly ensure the 
                    //       calling code to avoid this happening. 
                    var count = inner.Fill(matches);
                    if (count == 0)
                        count = outer.Fill(matches);
                    return count;
                }

                var bufferHolder = QueryContext.MatchesRawPool.Rent(sizeof(long) * matches.Length);
                var buffer = MemoryMarshal.Cast<byte, long>(bufferHolder).Slice(0, matches.Length);                

                // RavenDB-17750: The basic concept for this fill function is that we do not really care from which side the matches come
                //                but we need somewhat ensure that we are conceptually getting as small amount of overlapping matches on 
                //                different calls as possible. 

                int totalLength = buffer.Length;
                int idx = totalLength / 2;

                var innerSlice = buffer.Slice(0, idx);
                int innerCount = inner.Fill(innerSlice);
                var outerSlice = buffer.Slice(idx, idx);
                int outerCount = outer.Fill(outerSlice);
                Debug.Assert(innerSlice.Length == outerSlice.Length);

                // If we depleted everything we are done. 
                if (innerCount == 0 && outerCount == 0)
                {
                    idx = 0;
                    goto END; // Nothing more to do here, we are done. 
                }

                // We need to know if we have to run another who is gonna come next. The heuristic is to take the one with the smaller
                // index, so we can ensure to get as many repeated documents as possible in the batch. The rationale of why we do this
                // is to avoid having to do repeated work higher up in the query. 

                long innerMax = innerCount == 0 ? long.MaxValue : innerSlice[innerCount - 1];
                long outerMax = outerCount == 0 ? long.MaxValue : outerSlice[outerCount - 1];

                bool isOuterNext;
                if (innerCount == 0)
                    isOuterNext = true;
                else if (outerCount == 0)
                    isOuterNext = false;
                else
                    isOuterNext = innerMax > outerMax;               

                // This is the first run, merge on external buffer and copy back.
                idx = MergeHelper.Or(matches, innerSlice.Slice(0, innerCount), outerSlice.Slice(0, outerCount));                
                if (idx == matches.Length)
                    return idx; // The buffer is full, we are done. 

                // Most of the times we may not need to execute this part, specially when the matches are unique among sets.
                // Also when faced with smaller buffers it will execute until they are filled.
                var leftoverMatches = buffer.Slice(idx);
                while (leftoverMatches.Length > (totalLength / 32))
                {
                    // We use 1/32th of the buffer as an heuristic of how many calls we believe it would make sense to do to fill up the
                    // buffer vs providing more documents to the aggregations upper in the query tree. This can change, but at 4Kb buffers
                    // 128 empty places is a good enough tradeoff. 

                    if (innerCount == 0 && outerCount == 0)
                        break; // Nothing more to do here, we are done. 

                    int newIdx;
                    if (isOuterNext && outerCount != 0)
                    {
                        outerCount = outer.Fill(leftoverMatches);
                        newIdx = outerCount;
                        if (outerCount != 0)
                        {
                            outerMax = leftoverMatches[outerCount - 1];

                            // The idea here is to always use the one that hasnt yet seen the highest match. In this case
                            // if the current values are higher than the highest we found on the outer fill, then we continue on the outer. 
                            isOuterNext = innerMax > outerMax;
                        }
                        else
                        {
                            isOuterNext = false;
                        }
                    }
                    else
                    {
                        innerCount = inner.Fill(leftoverMatches);                        
                        newIdx = innerCount;
                        if (innerCount != 0)
                        {
                            innerMax = leftoverMatches[innerCount - 1];

                            // The idea here is to always use the one that hasnt yet seen the highest match. In this case
                            // if the current values are higher than the highest we found on the outer fill, then we continue on the outer. 
                            isOuterNext = innerMax > outerMax;
                        }                            
                        else
                        {
                            isOuterNext = true;
                        }
                    }

                    if ( newIdx != 0)
                    {
                        // Copy the result of matches to the buffer.
                        matches.Slice(0, idx).CopyTo(buffer);
                        idx = MergeHelper.Or(matches, buffer.Slice(0, idx), leftoverMatches.Slice(0, newIdx));                        

                        leftoverMatches = buffer.Slice(idx);
                    }
                }

                END:
                QueryContext.MatchesRawPool.Return(bufferHolder);
                return idx;
            }      

            static QueryInspectionNode InspectFunc(ref BinaryMatch<TInner, TOuter> match)
            {
                return new QueryInspectionNode($"{nameof(BinaryMatch)} [Or]",
                    children: new List<QueryInspectionNode> { match._inner.Inspect(), match._outer.Inspect() },
                    parameters: new Dictionary<string, string>()
                    {
                        { nameof(match.IsBoosting), match.IsBoosting.ToString() },
                        { nameof(match.Count), $"{match.Count} [{match.Confidence}]" }
                    });
            }

            // Estimate Confidence values.
            QueryCountConfidence confidence;
            if (inner.Count / 10 > outer.Count)
                confidence = inner.Confidence;
            else if (outer.Count / 10 > inner.Count)
                confidence = outer.Confidence;
            else
                confidence = inner.Confidence.Min(outer.Confidence);

            return new BinaryMatch<TInner, TOuter>(in inner, in outer, &FillFunc, &AndWith, &InspectFunc, inner.Count + outer.Count, confidence);
        }
    }
}
