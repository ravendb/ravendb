using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Voron;

namespace Corax.Queries
{

    unsafe partial struct UnaryMatch<TInner, TValueType>
    {
        public interface IUnaryMatchComparer
        {
            bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy);
            bool Compare<T>(T sx, T sy) where T : unmanaged;
        }

        [SkipLocalsInit]
        private static int AndWith(ref UnaryMatch<TInner, TValueType> match, Span<long> buffer, int matches)
        {
            var bufferHolder = QueryContext.MatchesRawPool.Rent(sizeof(long) * buffer.Length);
            var innerBuffer = MemoryMarshal.Cast<byte, long>(bufferHolder).Slice(0, buffer.Length);
            Debug.Assert(innerBuffer.Length == buffer.Length);

            var count = match._fillFunc(ref match, innerBuffer);

            var matchesPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer));
            var baseMatchesPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(innerBuffer));
            var result = MergeHelper.And(matchesPtr, buffer.Length, matchesPtr, matches, baseMatchesPtr, count);

            QueryContext.MatchesRawPool.Return(bufferHolder);
            return result;
        }



        [SkipLocalsInit]
        private static int FillFuncSequenceAny<TComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
            where TComparer : struct, IUnaryMatchComparer
        {
            // If we query unary, we want to find items where at least one element is valid for our conditions, so we look for the first match.
            // For example: Terms [1,2,3] Q: 'where Term < 3'. We check '1 < 3' and thats it.
            // Because the whole process is about comparing values, there is really no reason why we would call this when the condition is a null
            // value, so if we want to compare against a null (which is pretty common) a more optimized version of FillFuncSequenceAny would be needed.

            var searcher = match._searcher;
            var currentType = ((Slice)(object)match._value).AsReadOnlySpan();

            var comparer = default(TComparer);
            var currentMatches = matches;
            int totalResults = 0;
            int storeIdx = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(freeMemory[i]);
                    var type = reader.GetFieldType(match._fieldId, out var _);

                    var answer = match._distinct == false;
                    var isMatch = match._distinct;

                    // If we get a null, we just skip it. It will not match.
                    if (type == IndexEntryFieldType.Null)
                    {
                        isMatch = answer;
                    }                        
                    else if (type.HasFlag(IndexEntryFieldType.List) || type.HasFlag(IndexEntryFieldType.TupleList))
                    {
                        var iterator = reader.ReadMany(match._fieldId);

                        while (iterator.ReadNext())
                        {
                            if (iterator.IsNull)
                                continue;

                            var analyzedTerm = match._searcher.ApplyAnalyzer(iterator.Sequence, match._fieldId);
                            if (comparer.Compare(currentType, analyzedTerm))
                            {
                                isMatch = answer;
                                break;
                            }
                        }
                    }
                    else if (type.HasFlag(IndexEntryFieldType.Tuple) || type.HasFlag(IndexEntryFieldType.Simple))
                    {
                        var read = reader.Read(match._fieldId, out var readType, out var resultX);
                        if (read && readType != IndexEntryFieldType.Null)
                        {
                            var analyzedTerm = match._searcher.ApplyAnalyzer(resultX, match._fieldId);
                            if (read && comparer.Compare(currentType, analyzedTerm))
                            {
                                // We found a match.
                                isMatch = answer;
                            }
                        }
                    }

                    if (isMatch)
                    {
                        currentMatches[storeIdx] = freeMemory[i];
                        storeIdx++;
                        totalResults++;
                    }
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return storeIdx;
        }

        [SkipLocalsInit]
        private static int FillFuncSequenceAll<TComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
            where TComparer : struct, IUnaryMatchComparer
        {
            // If we query unary, we want to find items where at all elements has valid conditions, so we look for all the matches.            
            // The typical use of all is on distinct, we want to know if there are any element that matches the negative. 
            // For example: Terms [1,2,3] Q: 'where Term != 3'. We check every item to ensure there is no match with 3 there.
            // Because the whole process is about comparing values, there is really no reason why we would call this when the condition is a null
            // value, so if we want to compare against a null (which is pretty common) a more optimized version of FillFuncSequenceAny would be needed.

            var searcher = match._searcher;
            var currentType = ((Slice)(object)match._value).AsReadOnlySpan();

            var comparer = default(TComparer);
            var currentMatches = matches;
            int totalResults = 0;
            int storeIdx = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                var answer = match._distinct == false;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(freeMemory[i]);
                    var type = reader.GetFieldType(match._fieldId, out var _);

                    var isMatch = match._distinct;

                    // If we get a null, we just skip it. It will not match.
                    if (type != IndexEntryFieldType.Null)
                    {
                        if (type.HasFlag(IndexEntryFieldType.List) || type.HasFlag(IndexEntryFieldType.TupleList))
                        {
                            var iterator = reader.ReadMany(match._fieldId);

                            while (iterator.ReadNext())
                            {
                                // Null here is complicated. When we are in the positive case, we know that the value is not null,
                                // therefore finding a null in the case of the positive case would ensure that this is not a match.
                                // However, when we are in the negative case (distict), we know for certain it is different to the
                                // value and therefore we can continue to the next. 
                                if (iterator.IsNull && !match._distinct)
                                {
                                    isMatch = answer;
                                    break;
                                }

                                var analyzedTerm = match._searcher.ApplyAnalyzer(iterator.Sequence, match._fieldId);
                                if (comparer.Compare(currentType, analyzedTerm))
                                {
                                    isMatch = answer;
                                    break;
                                }
                            }
                        }
                        else if (type.HasFlag(IndexEntryFieldType.Tuple) || type.HasFlag(IndexEntryFieldType.Simple))
                        {
                            var read = reader.Read(match._fieldId, out var readType, out var resultX);
                            if (read && readType != IndexEntryFieldType.Null)
                            {
                                var analyzedTerm = match._searcher.ApplyAnalyzer(resultX, match._fieldId);
                                if (read && comparer.Compare(currentType, analyzedTerm))
                                {
                                    // We found a match for the inverse.
                                    isMatch = answer;
                                }
                            }
                        }
                    }                        

                    if (isMatch)
                    {
                        currentMatches[storeIdx] = freeMemory[i];
                        storeIdx++;
                        totalResults++;
                    }
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return storeIdx;
        }


        [SkipLocalsInit]
        private static int FillFuncAllNull(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
        {
            // If we query unary, we want to find if all items are null (most likely wont have much positives on lists), but
            // it can certainly be more common in tuples.
            // Since null is an special case, many of the more general comparisons 

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;
            int storeIdx = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                var answer = match._distinct == false;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(freeMemory[i]);
                    var type = reader.GetFieldType(match._fieldId, out var _);

                    var isMatch = match._distinct;

                    if (type != IndexEntryFieldType.Null)
                    {
                        if (type.HasFlag(IndexEntryFieldType.List) || type.HasFlag(IndexEntryFieldType.TupleList))
                        {
                            if (type.HasFlag(IndexEntryFieldType.HasNulls) && !type.HasFlag(IndexEntryFieldType.EmptyList))
                            {
                                var iterator = reader.ReadMany(match._fieldId);
                                while (iterator.ReadNext())
                                {
                                    if (iterator.IsNull)
                                    {
                                        isMatch = answer;
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                isMatch = answer;
                            }
                        }
                        else if (type.HasFlag(IndexEntryFieldType.Tuple) || type.HasFlag(IndexEntryFieldType.Simple))
                        {
                            var readType = reader.GetFieldType(match._fieldId, out var _);
                            if (readType != IndexEntryFieldType.Null)
                                isMatch = answer;
                        }
                    }

                    if (isMatch)
                    {
                        currentMatches[storeIdx] = freeMemory[i];
                        storeIdx++;
                        totalResults++;
                    }
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return storeIdx;
        }

        [SkipLocalsInit]
        private static int FillFuncAnyNull(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
        {
            // If we query unary, we want to find if all items are null (most likely wont have much positives on lists), but
            // it can certainly be more common in tuples.
            // Since null is an special case, many of the more general comparisons 

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;
            int storeIdx = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                var answer = match._distinct == false;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(freeMemory[i]);
                    var type = reader.GetFieldType(match._fieldId, out var _);

                    var isMatch = match._distinct;

                    if (type == IndexEntryFieldType.Null)
                    {
                        isMatch = answer;
                    }
                    else if (type.HasFlag(IndexEntryFieldType.List) || type.HasFlag(IndexEntryFieldType.TupleList))
                    {
                        if (type.HasFlag(IndexEntryFieldType.HasNulls) && !type.HasFlag(IndexEntryFieldType.EmptyList))
                        {
                            var iterator = reader.ReadMany(match._fieldId);
                            while (iterator.ReadNext())
                            {
                                if (iterator.IsNull)
                                {
                                    isMatch = answer;
                                    break;
                                }
                            }
                        }
                    }
                    else if (type.HasFlag(IndexEntryFieldType.Tuple) || type.HasFlag(IndexEntryFieldType.Simple))
                    {
                        var readType = reader.GetFieldType(match._fieldId, out var _);
                        if (readType == IndexEntryFieldType.Null)
                            isMatch = answer;
                    }

                    if (isMatch)
                    {
                        currentMatches[storeIdx] = freeMemory[i];
                        storeIdx++;
                        totalResults++;
                    }
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return storeIdx;
        }

        [SkipLocalsInit]
        private static int FillFuncNumerical<TComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
            where TComparer : struct, IUnaryMatchComparer
        {
            var currentType = match._value;

            var comparer = default(TComparer);
            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            int storeIdx = 0;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);
                if (results == 0)
                    return totalResults;

                var answer = match._distinct == false;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(freeMemory[i]);
                    var type = reader.GetFieldType(match._fieldId, out _);
                    
                    var isMatch = match._distinct;
                    
                    if (typeof(TValueType) == typeof(long))
                    {
                        if (type.HasFlag(IndexEntryFieldType.Tuple) || type.HasFlag(IndexEntryFieldType.Simple))
                        {
                            var read = reader.Read<long>(match._fieldId, out var resultX);
                            if (read)
                                isMatch = comparer.Compare((long)(object)currentType, resultX);
                        }
                        else if (type.HasFlag(IndexEntryFieldType.List) || type.HasFlag(IndexEntryFieldType.TupleList))
                        {
                            var iterator = reader.ReadMany(match._fieldId);
                            while (iterator.ReadNext())
                            {
                                if (comparer.Compare((long)(object)currentType, iterator.Long) == answer)
                                {
                                    isMatch = answer;
                                    break;
                                }
                            }
                        }
                    }
                    else if (typeof(TValueType) == typeof(double))
                    {
                        if (type.HasFlag(IndexEntryFieldType.Tuple) || type.HasFlag(IndexEntryFieldType.Simple))
                        {
                            var read = reader.Read<double>(match._fieldId, out var resultX);
                            if (read)
                                isMatch = comparer.Compare((double)(object)currentType, resultX);
                        }
                        else if (type.HasFlag(IndexEntryFieldType.List) || type.HasFlag(IndexEntryFieldType.TupleList))
                        {
                            var iterator = reader.ReadMany(match._fieldId);

                            while (iterator.ReadNext())
                            {
                                if (comparer.Compare((double)(object)currentType, iterator.Double) == answer)
                                {
                                    isMatch = answer;
                                    break;
                                }
                            }
                        }
                    }

                    if (isMatch)
                    {
                        // We found a match.
                        currentMatches[storeIdx] = freeMemory[i];
                        storeIdx++;
                        totalResults++;
                    }
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            matches = currentMatches.Slice(0, storeIdx);
            return totalResults;
        }

        public static UnaryMatch<TInner, TValueType> YieldIsNull(in TInner inner, IndexSearcher searcher, int fieldId, UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            return new UnaryMatch<TInner, TValueType>(
                in inner, UnaryMatchOperation.Equals,
                searcher, fieldId, default(TValueType),
                mode == UnaryMatchOperationMode.Any ? &FillFuncAnyNull : &FillFuncAllNull, &AndWith,
                inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, false, take: take);
        }

        public static UnaryMatch<TInner, TValueType> YieldIsNotNull(in TInner inner, IndexSearcher searcher, int fieldId, UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            return new UnaryMatch<TInner, TValueType>(
                in inner, UnaryMatchOperation.NotEquals,
                searcher, fieldId, default(TValueType),
                mode == UnaryMatchOperationMode.Any ? &FillFuncAnyNull : &FillFuncAllNull, &AndWith,
                inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, true, take: take);
        }

        public static UnaryMatch<TInner, TValueType> YieldGreaterThan(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value, UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any,
            int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {                
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThan,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<GreaterThanMatchComparer> : &FillFuncSequenceAll<GreaterThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThan,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumerical<GreaterThanMatchComparer> : &FillFuncNumerical<GreaterThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldGreaterThanOrEqualMatch(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThanOrEqual,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<GreaterThanOrEqualMatchComparer> : &FillFuncSequenceAll<GreaterThanOrEqualMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThanOrEqual,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumerical<GreaterThanOrEqualMatchComparer> : &FillFuncNumerical<GreaterThanOrEqualMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldLessThan(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value, UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any,
            int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThan,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<LessThanMatchComparer> : &FillFuncSequenceAll<LessThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThan,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumerical<LessThanMatchComparer> : &FillFuncNumerical<LessThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldLessThanOrEqualMatch(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThanOrEqual,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<LessThanOrEqualMatchComparer> : &FillFuncSequenceAll<LessThanOrEqualMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThanOrEqual,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumerical<LessThanOrEqualMatchComparer> : &FillFuncNumerical<LessThanOrEqualMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldNotEqualsMatch(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotEquals,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<EqualsMatchComparer> : &FillFuncSequenceAll<EqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, true, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotEquals,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumerical<EqualsMatchComparer> : &FillFuncNumerical<EqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, true, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldEqualsMatch(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.Equals,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<EqualsMatchComparer> : &FillFuncSequenceAll<EqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.Equals,
                    searcher, fieldId, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumerical<EqualsMatchComparer> : &FillFuncNumerical<EqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        internal struct GreaterThanMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) > 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy - (long)(object)sx) > 0;
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy - (double)(object)sx) > 0;

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }

        internal struct GreaterThanOrEqualMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) >= 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy - (long)(object)sx) >= 0;
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy - (double)(object)sx) >= 0;

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }

        internal struct LessThanMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) < 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy - (long)(object)sx) < 0;
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy - (double)(object)sx) < 0;

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }

        internal struct LessThanOrEqualMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) <= 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy - (long)(object)sx) <= 0;
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy - (double)(object)sx) <= 0;

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }

        internal struct NotEqualsMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) != 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy != (long)(object)sx);
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy != (double)(object)sx);

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }

        internal struct EqualsMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) == 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy == (long)(object)sx);
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy == (double)(object)sx);

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }
    }
}
