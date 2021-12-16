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
                    var results = inner.Fill(matches);
                    if (results == 0)
                        return 0;

                    results = outer.AndWith(matches.Slice(0, results));
                    if (results != 0)
                        return results;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWith(ref BinaryMatch<TInner, TOuter> match, Span<long> matches)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                var results = inner.AndWith(matches);

                return outer.AndWith(matches.Slice(0, results));
            }

            // Estimate Confidence values.
            QueryCountConfidence confidence;
            if (inner.Count < outer.Count / 2)
                confidence = inner.Confidence;
            else if (outer.Count < inner.Count / 2)
                confidence = outer.Confidence;
            else
                confidence = inner.Confidence.Min(outer.Confidence);

            return new BinaryMatch<TInner, TOuter>(in inner, in outer, &FillFunc, &AndWith, Math.Min(inner.Count, outer.Count), confidence);
        }

        public static BinaryMatch<TInner, TOuter> YieldOr(in TInner inner, in TOuter outer)
        {
            [SkipLocalsInit]
            static int AndWith(ref BinaryMatch<TInner, TOuter> match, Span<long> matches)
            {
                int matchesSize = matches.Length;

                var bufferHolder = QueryContext.MatchesPool.Rent(sizeof(long) * matchesSize);
                var orMatches = MemoryMarshal.Cast<byte, long>(bufferHolder).Slice(0, matchesSize);

                var count = FillFunc(ref match, orMatches);

                var matchesPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(matches));


                var orMatchesPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(orMatches));

                var result = MergeHelper.And(matchesPtr, matchesSize, matchesPtr, matchesSize, orMatchesPtr, count);
                
                QueryContext.MatchesPool.Return(bufferHolder);

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

                var bufferHolder = QueryContext.MatchesPool.Rent(sizeof(long) * matches.Length);
                var longBuffer = MemoryMarshal.Cast<byte, long>(bufferHolder);

                // need to be ready to put both outputs to the matches
                int length = matches.Length / 2;           
                Span<long> innerMatches = longBuffer.Slice(0, length);
                Debug.Assert(innerMatches.Length == length);
                Span<long> outerMatches = longBuffer.Slice(length, length);
                Debug.Assert(outerMatches.Length == length);

                var innerCount = inner.Fill(innerMatches);
                var outerCount = outer.Fill(outerMatches);

                long* innerMatchesPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(innerMatches));
                long* outerMatchesPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(outerMatches));
                long* matchesStartPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(matches));

                if (innerCount == 0)
                {
                    Unsafe.CopyBlockUnaligned(matchesStartPtr, outerMatchesPtr, (uint)outerCount * sizeof(long));
                    return outerCount;
                }
                if (outerCount == 0)
                {
                    Unsafe.CopyBlockUnaligned(matchesStartPtr, innerMatchesPtr, (uint)innerCount * sizeof(long));
                    return innerCount;
                }

                var innerMatchesPtrEnd = innerMatchesPtr + innerCount;
                var outerMatchesPtrEnd = outerMatchesPtr + outerCount;

                long* matchesPtr = matchesStartPtr;
                long* matchesPtrEnd = matchesStartPtr + matches.Length;

                while (innerMatchesPtr < innerMatchesPtrEnd && outerMatchesPtr < outerMatchesPtrEnd)
                {
                    Debug.Assert(matchesPtr >= matchesStartPtr && matchesPtr < matchesPtrEnd);

                    long innerMatch = *innerMatchesPtr;
                    long outerMatch = *outerMatchesPtr;

                    // PERF: The if-then-else version is actually faster than the branchless version because the
                    //       JIT wont generate a CMOV operation and there is no intrinsic yet available;
                    if (innerMatch == outerMatch)
                    {
                        *matchesPtr = *innerMatchesPtr;
                        innerMatchesPtr++;
                        outerMatchesPtr++;
                    }
                    else if (innerMatch < outerMatch)
                    {
                        *matchesPtr = *innerMatchesPtr;
                        innerMatchesPtr++;
                    }
                    else
                    {
                        *matchesPtr = *outerMatchesPtr;
                        outerMatchesPtr++;
                    }

                    matchesPtr++;
                }

                if (innerMatchesPtr < innerMatchesPtrEnd)
                {
                    long values = innerMatchesPtrEnd - innerMatchesPtr;
                    Unsafe.CopyBlockUnaligned(matchesPtr, innerMatchesPtr, (uint)values * sizeof(long));
                    matchesPtr += values;
                }
                else if (outerMatchesPtr < outerMatchesPtrEnd)
                {
                    long values = outerMatchesPtrEnd - outerMatchesPtr;
                    Unsafe.CopyBlockUnaligned(matchesPtr, outerMatchesPtr, (uint)values * sizeof(long));
                    matchesPtr += values;
                }

                Debug.Assert(matchesPtr >= matchesStartPtr && matchesPtr <= matchesPtrEnd);
                var result = (int)(matchesPtr - matchesStartPtr);

                QueryContext.MatchesPool.Return(bufferHolder);

                return result;
            }

            // Estimate Confidence values.
            QueryCountConfidence confidence;
            if (inner.Count / 10 > outer.Count)
                confidence = inner.Confidence;
            else if (outer.Count / 10 > inner.Count)
                confidence = outer.Confidence;
            else
                confidence = inner.Confidence.Min(outer.Confidence);

            return new BinaryMatch<TInner, TOuter>(in inner, in outer, &FillFunc, &AndWith, inner.Count + outer.Count, confidence);
        }
    }
}
