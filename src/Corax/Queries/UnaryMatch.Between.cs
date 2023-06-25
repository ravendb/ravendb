using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Corax.Mappings;
using Sparrow;
using Voron;

namespace Corax.Queries
{
    unsafe partial struct UnaryMatch<TInner, TValueType>
    {
        [SkipLocalsInit]
        private static int FillFuncBetweenSequence<TLeftSideComparer, TRightSideComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
            where TLeftSideComparer : IUnaryMatchComparer
            where TRightSideComparer : IUnaryMatchComparer
        {
            TLeftSideComparer leftSideComparer = default;
            TRightSideComparer rightSideComparer = default;

            var currentType1 = ((Slice)(object)match._value).AsReadOnlySpan();
            var currentType2 = ((Slice)(object)match._valueAux).AsReadOnlySpan();

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;

            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            while (currentMatches.Length > maxUnusedMatchesSlots)
            {
                var results = match._inner.Fill(currentMatches);
                if (results == 0)
                    return totalResults;

                int storeIdx = 0;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetEntryTermsReader(currentMatches[i], ref lastPage);
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;
                        var resultX = reader.Current.Decoded();
                        if (leftSideComparer!.Compare(currentType1, resultX) && rightSideComparer!.Compare(currentType2, resultX))
                        {
                            // We found a match.
                            currentMatches[storeIdx] = currentMatches[i];
                            storeIdx++;
                        }
                    }
                }

                totalResults += storeIdx;
                if (totalResults > match._take)
                    break;

                currentMatches = currentMatches.Slice(storeIdx);
            }

            return totalResults;
        }

        [SkipLocalsInit]
        private static int FillFuncBetweenNumerical<TLeftSideComparer, TRightSideComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
            where TLeftSideComparer : IUnaryMatchComparer
            where TRightSideComparer : IUnaryMatchComparer
        {
            TLeftSideComparer leftSideComparer = default;
            TRightSideComparer rightSideComparer = default;

            var currentType1 = match._value;
            var currentType2 = match._valueAux;

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;  
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;


            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            while (currentMatches.Length > maxUnusedMatchesSlots)
            {
                var results = match._inner.Fill(currentMatches);
                if (results == 0)
                    return totalResults;

                int storeIdx = 0;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetEntryTermsReader(currentMatches[i], ref lastPage);
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot || 
                           reader.HasNumeric == false)
                            continue;
                        bool isMatch;
                        if (typeof(TValueType) == typeof(long))
                        {
                            isMatch  = leftSideComparer!.Compare((long)(object)currentType1, reader.CurrentLong) && rightSideComparer!.Compare((long)(object)currentType2, reader.CurrentLong);
                        }
                        else if (typeof(TValueType) == typeof(double))
                        {
                            isMatch  = leftSideComparer!.Compare((double)(object)currentType1, reader.CurrentDouble) && rightSideComparer!.Compare((double)(object)currentType2, reader.CurrentDouble);
                        }
                        else
                        {
                            throw new NotSupportedException(typeof(TValueType).FullName);
                        }
                        if (isMatch)
                        {
                            // We found a match.
                            matches[storeIdx] = currentMatches[i];
                            storeIdx++;
                            break;
                        }
                    }
                }

                totalResults += storeIdx;
                if (totalResults >= match._take)
                    break;

            }

            return totalResults;
        }

        public static UnaryMatch<TInner, TValueType> YieldBetweenMatch<TLeftSideComparer, TRightSideComparer>(in TInner inner, IndexSearcher searcher, FieldMetadata field, TValueType value1, TValueType value2, int take = -1)
            where TLeftSideComparer : IUnaryMatchComparer
            where TRightSideComparer : IUnaryMatchComparer
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                var vs1 = ((Slice)(object)value1).AsReadOnlySpan();
                var vs2 = ((Slice)(object)value2).AsReadOnlySpan();
                if (vs1.SequenceCompareTo(vs2) > 0)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }

                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.Between, 
                    searcher, field, value1, value2, 
                    &FillFuncBetweenSequence<TLeftSideComparer, TRightSideComparer>, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take: take);
            }
            else if (typeof(TValueType) == typeof(long))
            {
                var vs1 = (long)(object)value1;
                var vs2 = (long)(object)value2;
                if (vs1 > vs2)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }

                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.Between, 
                    searcher, field, value1, value2, 
                    &FillFuncBetweenNumerical<TLeftSideComparer, TRightSideComparer>, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take: take);
            }
            else
            {
                var vs1 = (double)(object)value1;
                var vs2 = (double)(object)value2;
                if (vs1 > vs2)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotBetween, 
                    searcher, field, value1, value2, 
                    &FillFuncBetweenNumerical<TLeftSideComparer, TRightSideComparer>, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take: take);
            }
        }

        [SkipLocalsInit]
        private static int FillFuncNotBetweenSequence<TLeftSideComparer, TRightSideComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
        where TLeftSideComparer : IUnaryMatchComparer
        where TRightSideComparer : IUnaryMatchComparer
        
        {
            TLeftSideComparer leftSideComparer = default;
            TRightSideComparer rightSideComparer = default;

            var currentType1 = ((Slice)(object)match._value).AsReadOnlySpan();
            var currentType2 = ((Slice)(object)match._valueAux).AsReadOnlySpan();
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;

            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            while (currentMatches.Length > maxUnusedMatchesSlots)
            {
                var results = match._inner.Fill(currentMatches);
                if (results == 0)
                    return totalResults;

                int storeIdx = 0;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetEntryTermsReader(currentMatches[i], ref lastPage);
                    var isMatch = true;
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;

                        var resultX = reader.Current.Decoded();
                        if (leftSideComparer!.Compare(currentType1, resultX) && rightSideComparer!.Compare(currentType2, resultX))
                        {
                            isMatch = false;
                            break;
                        }
                    }
                    if(isMatch ==false)
                        continue;
                    
                    currentMatches[storeIdx++] = currentMatches[i];
                }

                totalResults += storeIdx;
                if (totalResults >= match._take)
                    break;

                currentMatches = currentMatches.Slice(storeIdx);
            }

            return totalResults;
        }

        [SkipLocalsInit]
        private static int FillFuncNotBetweenNumerical<TLeftSideComparer, TRightSideComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
            where TLeftSideComparer : IUnaryMatchComparer
            where TRightSideComparer : IUnaryMatchComparer
        {
            TLeftSideComparer leftSideComparer = default;
            TRightSideComparer rightSideComparer = default;

            var currentType1 = match._value;
            var currentType2 = match._valueAux;

            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;
            long fieldRoot = match._searcher.GetLookupRootPage(match._field.FieldName);

            Page lastPage = default;

            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            while (currentMatches.Length > maxUnusedMatchesSlots)
            {
                var results = match._inner.Fill(currentMatches);
                if (results == 0)
                    return totalResults;

                int storeIdx = 0;
                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetEntryTermsReader(currentMatches[i], ref lastPage);
                    var isMatch = true;
                    while (reader.MoveNext())
                    {
                        if(reader.TermMetadata != fieldRoot)
                            continue;
                        bool curMatch;
                        if (typeof(TValueType) == typeof(long))
                        {
                            curMatch = leftSideComparer!.Compare((long)(object)currentType1, reader.CurrentLong) &&
                                       rightSideComparer!.Compare((long)(object)currentType2, reader.CurrentLong);
                        }
                        else if (typeof(TValueType) == typeof(double))
                        {
                            curMatch = leftSideComparer!.Compare((double)(object)currentType1, reader.CurrentDouble) &&
                                       rightSideComparer!.Compare((double)(object)currentType2, reader.CurrentDouble);
                        }
                        else
                        {
                            throw new NotSupportedException(typeof(TValueType).FullName);
                        }

                        if (curMatch)
                        {
                            isMatch = false;
                            break;
                        }
                    }
                    
                    if (isMatch)
                    {
                        matches[storeIdx++] = currentMatches[i];
                    }
                }

                totalResults += storeIdx;
                if (totalResults >= match._take)
                    break;                    

            }

            matches = currentMatches;
            return totalResults;
        }

        public static UnaryMatch<TInner, TValueType> YieldNotBetweenMatch<TLeftSideComparer, TRightSideComparer>(in TInner inner, IndexSearcher searcher, FieldMetadata field, TValueType value1, TValueType value2, int take = -1)
            where TLeftSideComparer : IUnaryMatchComparer
            where TRightSideComparer : IUnaryMatchComparer
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                var vs1 = ((Slice)(object)value1).AsReadOnlySpan();
                var vs2 = ((Slice)(object)value2).AsReadOnlySpan();
                if (vs1.SequenceCompareTo(vs2) > 0)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }

                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotBetween, 
                    searcher, field, value1, value2, 
                    &FillFuncNotBetweenSequence<TLeftSideComparer, TRightSideComparer>, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), UnaryMatchOperationMode.All, take: take);
            }
            else if (typeof(TValueType) == typeof(long))
            {
                var vs1 = (long)(object)value1;
                var vs2 = (long)(object)value2;
                if (vs1 > vs2)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }

                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotBetween, 
                    searcher, field, value1, value2, 
                    &FillFuncNotBetweenNumerical<TLeftSideComparer, TRightSideComparer>, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), UnaryMatchOperationMode.All, take: take);
            }
            else
            {
                var vs1 = (double)(object)value1;
                var vs2 = (double)(object)value2;
                if (vs1 > vs2)
                {
                    var aux = value1;
                    value2 = value1;
                    value1 = aux;
                }
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotBetween, 
                    searcher, field, value1, value2, 
                    &FillFuncNotBetweenNumerical<TLeftSideComparer, TRightSideComparer>, &AndWith, 
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), UnaryMatchOperationMode.All, take: take);
            }
        }
    }
}
