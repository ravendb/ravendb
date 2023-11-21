using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Sparrow.Server.Utils;

namespace Corax.Querying.Matches
{
    unsafe partial struct BinaryMatch<TInner, TOuter>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BinaryMatch<TInner, TOuter> YieldAnd(Querying.IndexSearcher searcher, in TInner inner, in TOuter outer, in CancellationToken token)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref BinaryMatch<TInner, TOuter> match, Span<long> matches)
            {
                match._token.ThrowIfCancellationRequested();
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;
                while (true)
                {
                    int totalResults = 0;

                    // PERF: An alternative implementation would be to perform OR in place. The upside is that every improvement on
                    //       OR would impact everywhere this happens, but the vectorized Sort may also tip the balance here. Another
                    //       good behavior would be that in cases of many duplicates we will have a better use of the buffer because
                    //       OR will also deduplicate on every call. 

                    var resultsSpan = matches;
                    while (resultsSpan.Length > 0)
                    {
                        // RavenDB-17750: We have to fill everything possible UNTIL there are no more matches available.
                        var results = inner.Fill(resultsSpan);
                        
                        if (results == 0)
                            break;
                        totalResults += results;

                        resultsSpan = resultsSpan.Slice(results);
                    }
                    
                    // The problem is that multiple Fill calls do not ensure that we will get a sequence of ordered
                    // values, therefore we must ensure that we get a 'sorted' sequence ensuring those happen.
                    if (match._inner.AttemptToSkipSorting() != SkipSortingResult.ResultsNativelySorted)
                    {
                        if (totalResults > 0)
                        {
                            totalResults = Sorting.SortAndRemoveDuplicates(matches[0..totalResults]);
                        }
                    }

                    if (totalResults == 0)
                    {
                        match._memoizedOuter?.Dispose();
                        match._memoizedOuter = null;
                        return 0;
                    }
                    
                    match._token.ThrowIfCancellationRequested();
                    
                    // We got more than the matches buffer, we'll need to call AndWith multiple times
                    // which can be really expensive, instead, let's memoize the outer and remember that 
                    if (resultsSpan.Length == 0 && match._memoizedOuter is null)
                    {
                        match._memoizedOuter = new MemoizationMatchProvider<TOuter>(match._indexSearcher, match._outer);
                        match._memoizedOuter.SortingRequired();
                    }

                    if (match._memoizedOuter != null)
                    {
                        Span<long> results = match._memoizedOuter.FillAndRetrieve();
                        totalResults = MergeHelper.And(matches, matches[..totalResults], results);
                    }
                    else
                    {
                        totalResults = outer.AndWith(matches, totalResults);
                    }

                    if (totalResults != 0)
                        return totalResults;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWith(ref BinaryMatch<TInner, TOuter> match, Span<long> buffer, int matches)
            {
                ref var inner = ref match._inner;
                match._token.ThrowIfCancellationRequested();
                var results = inner.AndWith(buffer, matches);
                if (results == 0)
                    return 0;

                ref var outer = ref match._outer;
                match._token.ThrowIfCancellationRequested();
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

            return new BinaryMatch<TInner, TOuter>(searcher, in inner, in outer, &FillFunc, &AndWith, &InspectFunc, Math.Min(inner.Count, outer.Count), confidence, SkipSortingResult.ResultsNativelySorted, token);
        }

        public static BinaryMatch<TInner, TOuter> YieldOr(Querying.IndexSearcher indexSearcher, in TInner inner, in TOuter outer, in CancellationToken token)
        {
#if !DEBUG
            [SkipLocalsInit]
#endif
            static int AndWith(ref BinaryMatch<TInner, TOuter> match, Span<long> buffer, int matches)
            {
                match._token.ThrowIfCancellationRequested();
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                using var _ = match._ctx.Allocate(2 * matches * sizeof(long), out var bufferHolder);
                
                Span<long> innerBuffer = MemoryMarshal.Cast<byte, long>(bufferHolder.ToSpan());
                var innerMatches = innerBuffer.Slice(0, matches);
                var outerMatches = innerBuffer.Slice(matches, matches);
                Debug.Assert(innerMatches.Length == matches);
                Debug.Assert(outerMatches.Length == matches);

                // Important that we only keep the actual matches. 
                var actualMatches = buffer.Slice(0, matches);

                // Execute the AndWith operation for each subpart of the query.               
                actualMatches.CopyTo(innerMatches);
                int innerSize = inner.AndWith(innerMatches, matches);

                match._token.ThrowIfCancellationRequested();
                actualMatches.CopyTo(outerMatches);
                int outerSize = outer.AndWith(outerMatches, matches);
                
                // Merge the hits from every side into the output buffer. 
                var result = MergeHelper.Or(buffer, innerMatches.Slice(0, innerSize), outerMatches.Slice(0, outerSize));                

                return result;
            }
#if !DEBUG
            [SkipLocalsInit]
#endif
            static int FillFunc(ref BinaryMatch<TInner, TOuter> match, Span<long> matches)
            {
                match._token.ThrowIfCancellationRequested();
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

                //var bufferHolder = QueryContext.MatchesPool.Rent(matches.Length);
                using var _ = match._ctx.Allocate(sizeof(long) * matches.Length, out var bufferHolder);
                Span<long> buffer = MemoryMarshal.Cast<byte, long>(bufferHolder.ToSpan());

                // RavenDB-17750: The basic concept for this fill function is that we do not really care from which side the matches come
                //                but we need somewhat ensure that we are conceptually getting as small amount of overlapping matches on 
                //                different calls as possible. That is why we will try to get as much as possible but no less from each side.

                int totalLength = 0;

                // The heuristic is that if we are requesting the 'last few' items we should do any
                // work to try to 'get a balanced load'. Therefore less that 128 items makes absolutely
                // no sense to try to balance as we would lose the ability to chunk items. 
                int firstChunkSize = buffer.Length < 128 ? buffer.Length : buffer.Length / 2;

                // We will try to get half of the matches from the first side.
                var innerSlice = buffer[..firstChunkSize];
                int innerCount = inner.Fill(innerSlice);

                // We will try to fill the rest of the matches from the second side as the first may not have used all of them
                var outerSlice = buffer[innerCount..];                                
                int outerCount = innerCount < buffer.Length ? outer.Fill(outerSlice) : 0;

                // If we depleted everything we are done. 
                if (innerCount == 0 && outerCount == 0)
                {
                    totalLength = 0;
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
                totalLength = MergeHelper.Or(matches, innerSlice.Slice(0, innerCount), outerSlice.Slice(0, outerCount));                
                if (totalLength == matches.Length)
                    goto END; // The buffer is full, we are done. 

                // Most of the times we may not need to execute this part, specially when the matches are unique among sets.
                // Also when faced with smaller buffers it will execute until they are filled.
                var leftoverMatches = buffer[totalLength..];

                // Given that we are going to be calling Fill, which would try to fill the buffer as much as it can there is no
                // reason why we are not going to try to fill it here but avoid going overboard for leftovers.
                // What this does is definining a minimum batch of work, if there is less than 16 elements to fill,
                // it is not worth it to continue trying because the fill call would be higher than the probability of filling
                // that block. Probably that number will increase in the future, so far 16 is just about right.
                while (leftoverMatches.Length > 16)
                {
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

                            // If the other is also zero, we are done. 
                            if (innerCount == 0)
                                goto END;
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

                            // If the other is also zero, we are done. 
                            if (outerCount == 0)
                                goto END;
                        }
                    }

                    if ( newIdx != 0)
                    {
                        // Copy the result of matches to the buffer.
                        matches[0..totalLength].CopyTo(buffer);
                        totalLength = MergeHelper.Or(matches, buffer[..totalLength], leftoverMatches[0..newIdx]);                                                
                        leftoverMatches = buffer[totalLength..];
                    }
                }

                END:
                return totalLength;
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

            return new BinaryMatch<TInner, TOuter>(indexSearcher, in inner, in outer, &FillFunc, &AndWith, &InspectFunc, inner.Count + outer.Count, confidence,
                // For OR, assume (Name = 'Mario' or endsWith(Name, 'o') 
                // We get Mario from the left, and get Arlo, Enzo and Nico from the right in one Fill()
                // and in the next, we get nothing from the left and Mario from the right. 
                // We _cannot_ ensure sorting for OR
                SkipSortingResult.SortingIsRequired, token);
        }
    }
}
