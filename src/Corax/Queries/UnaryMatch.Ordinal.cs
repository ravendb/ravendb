using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Mappings;
using Corax.Utils;
using Sparrow;
using Sparrow.Binary;
using Voron;
using Voron.Data.CompactTrees;

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
            using var _ = match._searcher.Allocator.Allocate(sizeof(long) * buffer.Length, out var bufferHolder);
            var innerBuffer = MemoryMarshal.Cast<byte, long>(bufferHolder.ToSpan());
            Debug.Assert(innerBuffer.Length == buffer.Length);

            var count = match._fillFunc(ref match, innerBuffer);

            fixed (long* matchesPtr = buffer, baseMatchesPtr = innerBuffer)
            {
                var result = MergeHelper.And(matchesPtr, buffer.Length, matchesPtr, matches, baseMatchesPtr, count);
                return result;
            }
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

            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;
            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetEntryTermsReader(freeMemory[i], ref lastPage);

                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;
                        if (comparer.Compare(currentType, reader.Current.Decoded()) == false) 
                            continue;
                        
                        currentMatches[storeIdx++] = freeMemory[i];
                        totalResults++;
                        break;
                    }
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return storeIdx;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static int BinarySearch(TermQueryItem[] item, CompactKey value)
        {
            int l = 0;
            int r = item.Length - 1;
            while (l <= r)
            {
                var pivot = (l + r) >> 1;
                switch (item[pivot].Item.Compare(value))
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

            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static int BinarySearch(TermQueryItem[] item, ReadOnlySpan<byte> value)
        {
            int l = 0;
            int r = item.Length - 1;
            while (l <= r)
            {
                var pivot = (l + r) >> 1;
                switch (item[pivot].Item.Decoded().SequenceCompareTo(value))
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

            return -1;
        }

        [SkipLocalsInit]
        private static unsafe int FillFuncSequenceAllIn(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
        {
            var value = ((TermQueryItem[])(object)match._value);
            var requiredSizeOfBitset = value.Length / sizeof(byte) + (value.Length % sizeof(byte) == 0 ? 0 : 1);
            byte* bitsetBuffer = stackalloc byte[requiredSizeOfBitset];
            var bitsetBufferAsSpan = new Span<byte>(bitsetBuffer, requiredSizeOfBitset);

            var bitset = new PtrBitVector(bitsetBuffer, requiredSizeOfBitset);
            
            var searcher = match._searcher;
            var field = match._field;
            var currentMatches = matches;
            int totalResults = 0;
            int storeIdx = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;
            
            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

             
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetEntryTermsReader(freeMemory[i], ref lastPage);

                    while (reader.MoveNext())
                    {
                        if (reader.TermMetadata != fieldRoot)
                            continue;
                        CheckAndSet(reader.Current.Decoded());
                    }

                    int b = 0;
                    for (; b < requiredSizeOfBitset; ++b)
                        if (bitset[b] == false)
                            break;

                    if (b != requiredSizeOfBitset)
                        continue;
                        
                    currentMatches[storeIdx++] = freeMemory[i];
                    totalResults++;

                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return storeIdx;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            void CheckAndSet(ReadOnlySpan<byte> readFromEntry, bool isNull = false)
            {
                int index;
                if (isNull)
                {
                    index = BinarySearch(value, Constants.NullValueSlice.AsSpan());
                }
                else
                {
                    using (searcher.ApplyAnalyzer(field, readFromEntry, out var analyzedTerm))
                    {

                        index = BinarySearch(value, analyzedTerm.AsSpan());
                    }
                        
                }
                if (index >= 0)
                    bitset.Set(index);
            }
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
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                for (int i = 0; i < results; i++)
                {
                    bool isNotMatch = false;
                    var reader = searcher.GetEntryTermsReader(freeMemory[i], ref lastPage);
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;
                        
                        if (comparer.Compare(currentType, reader.Current.Decoded()) == false)
                        {
                            isNotMatch = true;
                            break;
                        }
                    }
                    if(isNotMatch) continue;
                    currentMatches[storeIdx++] = freeMemory[i];
                    totalResults++;
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return storeIdx;
        }


        [SkipLocalsInit]
        private static int FillFuncAllNonNull(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
        {
            // If we query unary, we want to find if all items are null (most likely wont have much positives on lists), but
            // it can certainly be more common in tuples.
            // Since null is an special case, many of the more general comparisons 

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;
            int storeIdx = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;
            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetEntryTermsReader(freeMemory[i], ref lastPage);
                    
                    var isNotMatch = false;
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;
                        if (reader.TermId == -1) // TODO: this is wrong, need to figure out what this looks like 
                        {
                            isNotMatch = true;
                            break;
                        }
                    }
                    if(isNotMatch)
                        continue;
                    
                    currentMatches[storeIdx++] = freeMemory[i];
                    totalResults++;
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return storeIdx;
        }

        [SkipLocalsInit]
        private static int FillFuncAnyNonNull(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
        {
            // If we query unary, we want to find if all items are null (most likely wont have much positives on lists), but
            // it can certainly be more common in tuples.
            // Since null is an special case, many of the more general comparisons 

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;
            int storeIdx = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;
            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetEntryTermsReader(freeMemory[i], ref lastPage);
                    bool isMatch = false;
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;
                        if (reader.TermId != -1) // TODO: this is wrong, need to fix it
                        {
                            isMatch = true;
                            break;
                        }
                    }

                    if (isMatch == false)
                        continue;

                    currentMatches[storeIdx++] = freeMemory[i];
                    totalResults++;
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
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                for (int i = 0; i < results; i++)
                {
                    
                    var reader = searcher.GetEntryTermsReader(freeMemory[i], ref lastPage);
                    bool isMatch = true;
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;
                        if (reader.TermId != -1) // TODO: this is wrong, need to fix it
                        {
                            isMatch = false;
                            break;
                        }
                    }

                    if (isMatch == false)
                        continue;

                    currentMatches[storeIdx++] = freeMemory[i];
                    totalResults++;
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
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                for (int i = 0; i < results; i++)
                {
                      
                    var reader = searcher.GetEntryTermsReader(freeMemory[i], ref lastPage);
                    bool isMatch = false;
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;
                        if (reader.TermId == -1) // TODO: this is wrong, need to fix it
                        {
                            isMatch = true;
                            break;
                        }
                    }

                    if (isMatch == false)
                        continue;

                    currentMatches[storeIdx++] = freeMemory[i];
                    totalResults++;
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return storeIdx;
        }
        
        [SkipLocalsInit]
        private static int FillFuncNumericalAny<TComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
            where TComparer : struct, IUnaryMatchComparer
        {
            var comparer = default(TComparer);
            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            int storeIdx = 0;
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);
                if (results == 0)
                    return totalResults;

                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetEntryTermsReader(freeMemory[i], ref lastPage);
                    bool isMatch = false;
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;
                        if(reader.HasNumeric == false)
                            continue;
                        if (TypesHelper.IsInteger<TValueType>())
                        {
                            long currentType = CoherseValueTypeToLong(match._value);
                            if (comparer.Compare(currentType, reader.CurrentLong))
                            {
                                isMatch = true;
                                break;
                            }
                        }
                        else if (TypesHelper.IsFloatingPoint<TValueType>())
                        {
                            double currentType = CoherseValueTypeToDouble(match._value);
                            if (comparer.Compare(currentType, reader.CurrentDouble))
                            {
                                isMatch = true;
                                break;
                            }
                        }
                        else
                        {
                            throw new NotSupportedException($"Type '{typeof(TValueType).Name} is not supported. Only double and float are supported.");
                        }
                    }
                    if(isMatch ==false)
                        continue;
                    
                    // We found a match.
                    currentMatches[storeIdx++] = freeMemory[i];
                    totalResults++;
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return totalResults;
        }

        [SkipLocalsInit]
        private static int FillFuncNumericalAll<TComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
            where TComparer : struct, IUnaryMatchComparer
        {
            var comparer = default(TComparer);
            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            int storeIdx = 0;
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;
            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);
                if (results == 0)
                    return totalResults;
                
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetEntryTermsReader(freeMemory[i], ref lastPage);
                    bool isMatch = true;
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;
                        if(reader.HasNumeric == false)
                            continue;
                        if (TypesHelper.IsInteger<TValueType>())
                        {
                            long currentType = CoherseValueTypeToLong(match._value);
                            if (comparer.Compare(currentType, reader.CurrentLong) == false)
                            {
                                isMatch = false;
                                break;
                            }
                        }
                        else if (TypesHelper.IsFloatingPoint<TValueType>())
                        {
                            double currentType = CoherseValueTypeToDouble(match._value);
                            if (comparer.Compare(currentType, reader.CurrentDouble) == false)
                            {
                                isMatch = false;
                                break;
                            }
                        }
                        else
                        {
                            throw new NotSupportedException($"Type '{typeof(TValueType).Name} is not supported. Only double and float are supported.");
                        }
                    }
                    if(isMatch ==false)
                        continue;
                    
                    // We found a match.
                    currentMatches[storeIdx++] = freeMemory[i];
                    totalResults++;
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return totalResults;
        }

        public static UnaryMatch<TInner, TValueType> YieldIsNull(in TInner inner, IndexSearcher searcher, FieldMetadata field,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            return new UnaryMatch<TInner, TValueType>(
                in inner, UnaryMatchOperation.Equals,
                searcher, field, default(TValueType),
                mode == UnaryMatchOperationMode.Any ? &FillFuncAnyNull : &FillFuncAllNull, &AndWith,
                inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
        }

        public static UnaryMatch<TInner, TValueType> YieldIsNotNull(in TInner inner, IndexSearcher searcher, FieldMetadata field,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            return new UnaryMatch<TInner, TValueType>(
                in inner, UnaryMatchOperation.NotEquals,
                searcher, field, default(TValueType),
                mode == UnaryMatchOperationMode.Any ? &FillFuncAnyNonNull : &FillFuncAllNonNull, &AndWith,
                inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
        }

        public static UnaryMatch<TInner, TValueType> YieldGreaterThan(in TInner inner, IndexSearcher searcher, FieldMetadata field, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any,
            int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThan,
                    searcher, field, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<GreaterThanMatchComparer> : &FillFuncSequenceAll<GreaterThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThan,
                    searcher, field, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumericalAny<GreaterThanMatchComparer> : &FillFuncNumericalAll<GreaterThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldGreaterThanOrEqualMatch(in TInner inner, IndexSearcher searcher, FieldMetadata field, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThanOrEqual,
                    searcher, field, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<GreaterThanOrEqualMatchComparer> : &FillFuncSequenceAll<GreaterThanOrEqualMatchComparer>,
                    &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThanOrEqual,
                    searcher, field, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumericalAny<GreaterThanOrEqualMatchComparer> : &FillFuncNumericalAny<GreaterThanOrEqualMatchComparer>,
                    &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldLessThan(in TInner inner, IndexSearcher searcher, FieldMetadata field, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any,
            int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThan,
                    searcher, field, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<LessThanMatchComparer> : &FillFuncSequenceAll<LessThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThan,
                    searcher, field, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumericalAny<LessThanMatchComparer> : &FillFuncNumericalAll<LessThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldLessThanOrEqualMatch(in TInner inner, IndexSearcher searcher, FieldMetadata field, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThanOrEqual,
                    searcher, field, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<LessThanOrEqualMatchComparer> : &FillFuncSequenceAll<LessThanOrEqualMatchComparer>,
                    &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThanOrEqual,
                    searcher, field, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumericalAny<LessThanOrEqualMatchComparer> : &FillFuncNumericalAny<LessThanOrEqualMatchComparer>,
                    &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldNotEqualsMatch(in TInner inner, IndexSearcher searcher, FieldMetadata field, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotEquals,
                    searcher, field, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<NotEqualsMatchComparer> : &FillFuncSequenceAll<NotEqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotEquals,
                    searcher, field, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumericalAny<NotEqualsMatchComparer> : &FillFuncNumericalAll<NotEqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldEqualsMatch(in TInner inner, IndexSearcher searcher, FieldMetadata binding, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.Equals,
                    searcher, binding, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncSequenceAny<EqualsMatchComparer> : &FillFuncSequenceAll<EqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.Equals,
                    searcher, binding, value,
                    mode == UnaryMatchOperationMode.Any ? &FillFuncNumericalAny<EqualsMatchComparer> : &FillFuncNumericalAll<EqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldAllIn(in TInner inner, IndexSearcher searcher, FieldMetadata field, TValueType value,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any, int take = -1)
        {
            return new UnaryMatch<TInner, TValueType>(
                in inner, UnaryMatchOperation.AllIn,
                searcher, field, value,
                &FillFuncSequenceAllIn, &AndWith,
                inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), mode, take: take);
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
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long CoherseValueTypeToLong(TValueType value)
        {
            if (typeof(TValueType) == typeof(long))
                return (long)(object)value;
            if (typeof(TValueType) == typeof(ulong))
                return (long)(ulong)(object)value;
            if (typeof(TValueType) == typeof(int))
                return (long)(int)(object)value;
            if (typeof(TValueType) == typeof(uint))
                return (long)(uint)(object)value;
            if (typeof(TValueType) == typeof(short))
                return (long)(short)(object)value;
            if (typeof(TValueType) == typeof(ushort))
                return (long)(ushort)(object)value;
            if (typeof(TValueType) == typeof(byte))
                return (long)(byte)(object)value;
            if (typeof(TValueType) == typeof(sbyte))
                return (long)(sbyte)(object)value;

            throw new NotSupportedException($"Type '{typeof(TValueType).Name} is not supported. Only long, ulong, int, uint, double and float are supported.");
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static double CoherseValueTypeToDouble(TValueType value)
        {
            if (typeof(TValueType) == typeof(double))
                return (double)(object)value;
            if (typeof(TValueType) == typeof(float))
                return (double)(float)(object)value;

            throw new NotSupportedException($"Type '{typeof(TValueType).Name} is not supported. Only long, ulong, int, uint, double and float are supported.");
        }
    }
}
