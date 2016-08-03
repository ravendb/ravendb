using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Util;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Abstractions;
using Raven.Database.Config;
using Raven.Database.Server.Connections;
using Sparrow;
using Voron.Util;

namespace Raven.Database.Queries
{

    public class FacetedQueryRunner
    {
        private readonly DocumentDatabase database;

        public FacetedQueryRunner(DocumentDatabase database)
        {
            this.database = database;
        }

        public FacetResults GetFacets(string index, IndexQuery indexQuery, List<Facet> facets, int start = 0, int? pageSize = null)
        {
            var sp = Stopwatch.StartNew();
            var results = new FacetResults();
            var defaultFacets = new Dictionary<string, Facet>();
            var rangeFacets = new Dictionary<string, List<ParsedRange>>();

            var viewGenerator = database.IndexDefinitionStorage.GetViewGenerator(index);
            Index.AssertQueryDoesNotContainFieldsThatAreNotIndexed(indexQuery, viewGenerator);

            foreach (var facet in facets)
            {
                var key = string.IsNullOrWhiteSpace(facet.DisplayName) ? facet.Name : facet.DisplayName;

                defaultFacets[key] = facet;
                if (facet.Aggregation != FacetAggregation.Count && facet.Aggregation != FacetAggregation.None)
                {
                    if (string.IsNullOrEmpty(facet.AggregationField))
                        throw new InvalidOperationException("Facet " + facet.Name + " cannot have aggregation set to " +
                                                            facet.Aggregation + " without having a value in AggregationField");

                    if (facet.AggregationField.EndsWith("_Range") == false)
                    {
                        if (QueryForFacets.IsAggregationTypeNumerical(facet.AggregationType))
                            facet.AggregationField = facet.AggregationField + "_Range";
                    }

                }


                switch (facet.Mode)
                {
                    case FacetMode.Default:
                        results.Results[key] = new FacetResult();
                        break;
                    case FacetMode.Ranges:
                        rangeFacets[key] = facet.Ranges.Select(range => ParseRange(facet.Name, range)).ToList();
                        results.Results[key] = new FacetResult
                        {
                            Values = facet.Ranges.Select(range => new FacetValue
                            {
                                Range = range,
                            }).ToList()
                        };

                        break;
                    default:
                        throw new ArgumentException(string.Format("Could not understand '{0}'", facet.Mode));
                }
            }

            var queryForFacets = new QueryForFacets(database, index, defaultFacets, rangeFacets, indexQuery, results, start, pageSize);
            queryForFacets.Execute();
            results.Duration = sp.Elapsed;
            return results;
        }

        private static ParsedRange ParseRange(string field, string range)
        {
            var parts = range.Split(new[] { " TO " }, 2, StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length != 2)
                throw new ArgumentException("Could not understand range query: " + range);

            var trimmedLow = parts[0].Trim();
            var trimmedHigh = parts[1].Trim();
            var parsedRange = new ParsedRange
            {
                Field = field,
                RangeText = range,
                LowInclusive = IsInclusive(trimmedLow.First()),
                HighInclusive = IsInclusive(trimmedHigh.Last()),
                LowValue = trimmedLow.Substring(1),
                HighValue = trimmedHigh.Substring(0, trimmedHigh.Length - 1)
            };

            if (RangeQueryParser.NumericRangeValue.IsMatch(parsedRange.LowValue))
            {
                parsedRange.LowValue = NumericStringToSortableNumeric(parsedRange.LowValue);
            }

            if (RangeQueryParser.NumericRangeValue.IsMatch(parsedRange.HighValue))
            {
                parsedRange.HighValue = NumericStringToSortableNumeric(parsedRange.HighValue);
            }


            if (parsedRange.LowValue == "NULL" || parsedRange.LowValue == "*")
                parsedRange.LowValue = null;
            if (parsedRange.HighValue == "NULL" || parsedRange.HighValue == "*")
                parsedRange.HighValue = null;

            parsedRange.LowValue = UnescapeValueIfNecessary(parsedRange.LowValue);
            parsedRange.HighValue = UnescapeValueIfNecessary(parsedRange.HighValue);

            return parsedRange;
        }

        private static string UnescapeValueIfNecessary(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            var unescapedValue = QueryBuilder.Unescape(value);

            DateTime _;
            if (DateTime.TryParseExact(unescapedValue, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out _))
                return unescapedValue;

            return value;
        }

        private static string NumericStringToSortableNumeric(string value)
        {
            var number = NumberUtil.StringToNumber(value);
            if (number is int)
            {
                return NumericUtils.IntToPrefixCoded((int)number);
            }
            if (number is long)
            {
                return NumericUtils.LongToPrefixCoded((long)number);
            }
            if (number is float)
            {
                return NumericUtils.FloatToPrefixCoded((float)number);
            }
            if (number is double)
            {
                return NumericUtils.DoubleToPrefixCoded((double)number);
            }

            throw new ArgumentException("Unknown type for " + number.GetType() + " which started as " + value);
        }

        private static bool IsInclusive(char ch)
        {
            switch (ch)
            {
                case '[':
                case ']':
                    return true;
                case '{':
                case '}':
                    return false;
                default:
                    throw new ArgumentException("Could not understand range prefix: " + ch);
            }
        }

        public class ParsedRange
        {
            public bool LowInclusive;
            public bool HighInclusive;
            public string LowValue;
            public string HighValue;
            public string RangeText;
            public string Field;

            public bool IsMatch(string value)
            {
                var compareLow =
                    LowValue == null
                        ? -1
                        : string.CompareOrdinal(value, LowValue);
                var compareHigh = HighValue == null ? 1 : string.CompareOrdinal(value, HighValue);
                // if we are range exclusive on either end, check that we will skip the edge values
                if (compareLow == 0 && LowInclusive == false ||
                    compareHigh == 0 && HighInclusive == false)
                    return false;

                if (LowValue != null && compareLow < 0)
                    return false;

                if (HighValue != null && compareHigh > 0)
                    return false;

                return true;
            }

            public override string ToString()
            {
                return string.Format("{0}:{1}", Field, RangeText);
            }
        }

        public class QueryForFacets
        {
            private readonly IndexDefinition indexDefinition;
            DocumentDatabase Database { get; set; }
            string Index { get; set; }
            Dictionary<string, Facet> Facets { get; set; }
            Dictionary<string, List<ParsedRange>> Ranges { get; set; }
            IndexQuery IndexQuery { get; set; }
            FacetResults Results { get; set; }
            private int Start { get; set; }
            private int? PageSize { get; set; }
            private uint _fieldsCrc;
            private IndexSearcherHolder.IndexSearcherHoldingState _currentState;

            public QueryForFacets(
                DocumentDatabase database,
                string index,
                 Dictionary<string, Facet> facets,
                 Dictionary<string, List<ParsedRange>> ranges,
                 IndexQuery indexQuery,
                 FacetResults results,
                 int start,
                 int? pageSize)
            {
                Database = database;
                Index = index;
                Facets = facets;
                Ranges = ranges;
                IndexQuery = indexQuery;
                Results = results;
                Start = start;
                PageSize = pageSize;
                indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(Index);
            }


            public void Execute()
            {
                ValidateFacets();


                var facetsByName = new Dictionary<string, Dictionary<string, FacetValue>>();

                bool isDistinct = IndexQuery.IsDistinct;
                if (isDistinct)
                {
                    _fieldsCrc = IndexQuery.FieldsToFetch.Aggregate<string, uint>(0, (current, field) => Crc.Value(field, current));
                }

                _currentState = Database.IndexStorage.GetCurrentStateHolder(Index);
                using (_currentState)
                {
                    var currentIndexSearcher = _currentState.IndexSearcher;

                    var baseQuery = Database.IndexStorage.GetDocumentQuery(Index, IndexQuery, Database.IndexQueryTriggers);
                    var returnedReaders = GetQueryMatchingDocuments(currentIndexSearcher, baseQuery);

                    foreach (var facet in Facets.Values)
                    {
                        if (facet.Mode != FacetMode.Default)
                            continue;

                        Dictionary<string, HashSet<IndexSearcherHolder.StringCollectionValue>> distinctItems = null;
                        HashSet<IndexSearcherHolder.StringCollectionValue> alreadySeen = null;
                        if (isDistinct)
                            distinctItems = new Dictionary<string, HashSet<IndexSearcherHolder.StringCollectionValue>>();

                        foreach (var readerFacetInfo in returnedReaders)
                        {
                            var termsForField = IndexedTerms.GetTermsAndDocumentsFor(readerFacetInfo.Reader, readerFacetInfo.DocBase, facet.Name, Database.Name, Index);

                            Dictionary<string, FacetValue> facetValues;

                            if (facetsByName.TryGetValue(facet.DisplayName, out facetValues) == false)
                            {
                                facetsByName[facet.DisplayName] = facetValues = new Dictionary<string, FacetValue>();
                            }

                            foreach (var kvp in termsForField)
                            {
                                if (isDistinct)
                                {
                                    if (distinctItems.TryGetValue(kvp.Key, out alreadySeen) == false)
                                    {
                                        alreadySeen = new HashSet<IndexSearcherHolder.StringCollectionValue>();
                                        distinctItems[kvp.Key] = alreadySeen;
                                    }
                                }

                                var needToApplyAggregation = (facet.Aggregation == FacetAggregation.None || facet.Aggregation == FacetAggregation.Count) == false;
                                var intersectedDocuments = GetIntersectedDocuments(new ArraySegment<int>(kvp.Value), readerFacetInfo.Results, alreadySeen, needToApplyAggregation);
                                var intersectCount = intersectedDocuments.Count;
                                if (intersectCount == 0)
                                    continue;

                                FacetValue facetValue;
                                if (facetValues.TryGetValue(kvp.Key, out facetValue) == false)
                                {
                                    facetValue = new FacetValue
                                    {
                                        Range = GetRangeName(facet.Name, kvp.Key)
                                    };
                                    facetValues.Add(kvp.Key, facetValue);
                                }
                                facetValue.Hits += intersectCount;
                                facetValue.Count = facetValue.Hits;

                                if (needToApplyAggregation)
                                {
                                    var docsInQuery = new ArraySegment<int>(intersectedDocuments.Documents, 0, intersectedDocuments.Count);
                                    ApplyAggregation(facet, facetValue, docsInQuery, readerFacetInfo.Reader, readerFacetInfo.DocBase);
                                }
                            }
                        }
                    }

                    foreach (var range in Ranges)
                    {
                        var facet = Facets[range.Key];
                        var needToApplyAggregation = (facet.Aggregation == FacetAggregation.None || facet.Aggregation == FacetAggregation.Count) == false;

                        Dictionary<string, HashSet<IndexSearcherHolder.StringCollectionValue>> distinctItems = null;
                        HashSet<IndexSearcherHolder.StringCollectionValue> alreadySeen = null;
                        if (isDistinct)
                            distinctItems = new Dictionary<string, HashSet<IndexSearcherHolder.StringCollectionValue>>();

                        foreach (var readerFacetInfo in returnedReaders)
                        {
                            var termsForField = IndexedTerms.GetTermsAndDocumentsFor(readerFacetInfo.Reader, readerFacetInfo.DocBase, facet.Name, Database.Name, Index);
                            if (isDistinct)
                            {
                                if (distinctItems.TryGetValue(range.Key, out alreadySeen) == false)
                                {
                                    alreadySeen = new HashSet<IndexSearcherHolder.StringCollectionValue>();
                                    distinctItems[range.Key] = alreadySeen;
                                }
                            }

                            var facetResult = Results.Results[range.Key];
                            var ranges = range.Value;
                            foreach (var kvp in termsForField)
                            {
                                for (int i = 0; i < ranges.Count; i++)
                                {
                                    var parsedRange = ranges[i];
                                    if (parsedRange.IsMatch(kvp.Key))
                                    {
                                        var facetValue = facetResult.Values[i];

                                        var intersectedDocuments = GetIntersectedDocuments(new ArraySegment<int>(kvp.Value), readerFacetInfo.Results, alreadySeen, needToApplyAggregation);
                                        var intersectCount = intersectedDocuments.Count;
                                        if (intersectCount == 0)
                                            continue;

                                        facetValue.Hits += intersectCount;
                                        facetValue.Count = facetValue.Hits;

                                        if (needToApplyAggregation)
                                        {
                                            var docsInQuery = new ArraySegment<int>(intersectedDocuments.Documents, 0, intersectedDocuments.Count);
                                            ApplyAggregation(facet, facetValue, docsInQuery, readerFacetInfo.Reader, readerFacetInfo.DocBase);
                                            IntArraysPool.Instance.FreeArray(intersectedDocuments.Documents);
                                            intersectedDocuments.Documents = null;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    UpdateFacetResults(facetsByName);

                    CompleteFacetCalculationsStage();

                    foreach (var readerFacetInfo in returnedReaders)
                    {
                        IntArraysPool.Instance.FreeArray(readerFacetInfo.Results.Array);
                    }
                }
            }

            private List<ReaderFacetInfo> GetQueryMatchingDocuments(IndexSearcher currentIndexSearcher, Query baseQuery)
            {
                var gatherAllCollector = new GatherAllCollectorByReader();
                currentIndexSearcher.Search(baseQuery, gatherAllCollector);

                foreach (var readerFacetInfo in gatherAllCollector.Results)
                {
                    readerFacetInfo.Complete();
                }

                return gatherAllCollector.Results;
            }

            private class IntersectDocs
            {
                public int Count;
                public int[] Documents;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public void AddIntersection(int docId)
                {
                    if (Documents != null)
                    {
                        if (Count >= Documents.Length)
                        {
                            IncreaseSize();
                        }
                        Documents[Count] = docId;
                    }
                    Count++;
                }

                private void IncreaseSize()
                {
                    var newDocumentsArray = IntArraysPool.Instance.AllocateArray(Count * 2);
                    Array.Copy(Documents, newDocumentsArray, Count);
                    IntArraysPool.Instance.FreeArray(Documents);
                    Documents = newDocumentsArray;
                }
            }

            /// <summary>
            /// This method expects both lists to be sorted
            /// </summary>
            private IntersectDocs GetIntersectedDocuments(ArraySegment<int> a, ArraySegment<int> b, HashSet<IndexSearcherHolder.StringCollectionValue> alreadySeen, bool needToApplyAggregation)
            {
                ArraySegment<int> n, m;
                if (a.Count > b.Count)
                {
                    n = a;
                    m = b;
                }
                else
                {
                    n = b;
                    m = a;
                }

                int nSize = n.Count;
                int mSize = m.Count;

                double o1 = nSize + mSize;
                double o2 = nSize * Math.Log(mSize, 2);

                var isDistinct = IndexQuery.IsDistinct;
                var result = new IntersectDocs();
                if (needToApplyAggregation)
                {
                    result.Documents = IntArraysPool.Instance.AllocateArray();
                }

                if (o1 < o2)
                {
                    int mi = m.Offset, ni = n.Offset;
                    while (mi < mSize && ni < nSize)
                    {
                        var nVal = n.Array[ni];
                        var mVal = m.Array[mi];

                        if (nVal > mVal)
                        {
                            mi++;
                        }
                        else if (nVal < mVal)
                        {
                            ni++;
                        }
                        else
                        {
                            int docId = nVal;
                            if (isDistinct == false || IsDistinctValue(docId, alreadySeen))
                            {
                                result.AddIntersection(docId);
                            }

                            ni++;
                            mi++;
                        }
                    }
                }
                else
                {
                    for (int i = m.Offset; i < mSize; i++)
                    {
                        int docId = m.Array[i];
                        if (Array.BinarySearch(n.Array, n.Offset, n.Count, docId) >= 0)
                        {

                            if (isDistinct == false || IsDistinctValue(docId, alreadySeen))
                            {
                                result.AddIntersection(docId);
                            }
                        }
                    }
                }
                return result;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool IsDistinctValue(int docId, HashSet<IndexSearcherHolder.StringCollectionValue> alreadySeen)
            {
                var fields = _currentState.GetFieldsValues(docId, _fieldsCrc, IndexQuery.FieldsToFetch);
                return alreadySeen.Add(fields);
            }

            private void ValidateFacets()
            {
                foreach (var facet in Facets.Where(facet => IsAggregationNumerical(facet.Value.Aggregation) && IsAggregationTypeNumerical(facet.Value.AggregationType) && GetSortOptionsForFacet(facet.Value.AggregationField) == SortOptions.None))
                {
                    throw new InvalidOperationException(string.Format("Index '{0}' does not have sorting enabled for a numerical field '{1}'.", Index, facet.Value.AggregationField));
                }
            }

            private static bool IsAggregationNumerical(FacetAggregation aggregation)
            {
                switch (aggregation)
                {
                    case FacetAggregation.Average:
                    case FacetAggregation.Count:
                    case FacetAggregation.Max:
                    case FacetAggregation.Min:
                    case FacetAggregation.Sum:
                        return true;
                    default:
                        return false;
                }
            }

            public static bool IsAggregationTypeNumerical(string aggregationType)
            {
                if (aggregationType == null)
                    return false;
                var type = Type.GetType(aggregationType, false, true);
                if (type == null)
                    return false;

                var numericalTypes = new List<Type>
                                     {
                                         typeof(decimal),
                                         typeof(int),
                                         typeof(long),
                                         typeof(short),
                                         typeof(float),
                                         typeof(double)
                                     };

                return numericalTypes.Any(numericalType => numericalType == type);
            }

            private string GetRangeName(string field, string text)
            {
                var sortOptions = GetSortOptionsForFacet(field);
                switch (sortOptions)
                {
                    case SortOptions.None:
                    case SortOptions.String:
                    case SortOptions.StringVal:
                    case SortOptions.Custom:
                        return text;
                    case SortOptions.Int:
                        if (IsStringNumber(text))
                            return text;
                        return NumericUtils.PrefixCodedToInt(text).ToString(CultureInfo.InvariantCulture);
                    case SortOptions.Long:
                        if (IsStringNumber(text))
                            return text;
                        return NumericUtils.PrefixCodedToLong(text).ToString(CultureInfo.InvariantCulture);
                    case SortOptions.Double:
                        if (IsStringNumber(text))
                            return text;
                        return NumericUtils.PrefixCodedToDouble(text).ToString(CultureInfo.InvariantCulture);
                    case SortOptions.Float:
                        if (IsStringNumber(text))
                            return text;
                        return NumericUtils.PrefixCodedToFloat(text).ToString(CultureInfo.InvariantCulture);
                    case SortOptions.Byte:
                    case SortOptions.Short:
                    default:
                        throw new ArgumentException("Can't get range name from sort option" + sortOptions);
                }
            }

            private bool IsStringNumber(string value)
            {
                if (value == null || string.IsNullOrEmpty(value))
                    return false;
                return char.IsDigit(value[0]);
            }

            private void CompleteFacetCalculationsStage()
            {
                foreach (var facetResult in Results.Results)
                {
                    var key = facetResult.Key;
                    foreach (var facet in Facets.Values.Where(f => f.DisplayName == key))
                    {
                        if (facet.Aggregation.HasFlag(FacetAggregation.Average))
                        {
                            foreach (var facetValue in facetResult.Value.Values)
                            {
                                if (facetValue.Hits == 0)
                                    facetValue.Average = double.NaN;
                                else
                                    facetValue.Average = facetValue.Average / facetValue.Hits;
                            }
                        }
                    }
                }
            }

            private void ApplyAggregation(Facet facet, FacetValue value, ArraySegment<int> docsInQuery, IndexReader indexReader, int docBase)
            {
                var sortOptionsForFacet = GetSortOptionsForFacet(facet.AggregationField);
                switch (sortOptionsForFacet)
                {
                    case SortOptions.String:
                    case SortOptions.StringVal:
                    case SortOptions.Byte:
                    case SortOptions.Short:
                    case SortOptions.Custom:
                    case SortOptions.None:
                        throw new InvalidOperationException(string.Format("Cannot perform numeric aggregation on index field '{0}'. You must set the Sort mode of the field to Int, Float, Long or Double.", TryTrimRangeSuffix(facet.AggregationField)));
                    case SortOptions.Int:
                        int[] ints = FieldCache_Fields.DEFAULT.GetInts(indexReader, facet.AggregationField);
                        for (int index = 0; index < docsInQuery.Count; index++)
                        {
                            var doc = docsInQuery.Array[index];
                            var currentVal = ints[doc - docBase];
                            if (facet.Aggregation.HasFlag(FacetAggregation.Max))
                            {
                                value.Max = Math.Max(value.Max ?? Double.MinValue, currentVal);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Min))
                            {
                                value.Min = Math.Min(value.Min ?? Double.MaxValue, currentVal);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Sum))
                            {
                                value.Sum = currentVal + (value.Sum ?? 0d);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Average))
                            {
                                value.Average = currentVal + (value.Average ?? 0d);
                            }
                        }
                        break;
                    case SortOptions.Float:
                        var floats = FieldCache_Fields.DEFAULT.GetFloats(indexReader, facet.AggregationField);
                        for (int index = 0; index < docsInQuery.Count; index++)
                        {
                            var doc = docsInQuery.Array[index];
                            var currentVal = floats[doc - docBase];
                            if (facet.Aggregation.HasFlag(FacetAggregation.Max))
                            {
                                value.Max = Math.Max(value.Max ?? Double.MinValue, currentVal);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Min))
                            {
                                value.Min = Math.Min(value.Min ?? Double.MaxValue, currentVal);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Sum))
                            {
                                value.Sum = currentVal + (value.Sum ?? 0d);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Average))
                            {
                                value.Average = currentVal + (value.Average ?? 0d);
                            }
                        }
                        break;
                    case SortOptions.Long:
                        var longs = FieldCache_Fields.DEFAULT.GetLongs(indexReader, facet.AggregationField);
                        for (int index = 0; index < docsInQuery.Count; index++)
                        {
                            var doc = docsInQuery.Array[index];

                            var currentVal = longs[doc - docBase];
                            if (facet.Aggregation.HasFlag(FacetAggregation.Max))
                            {
                                value.Max = Math.Max(value.Max ?? Double.MinValue, currentVal);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Min))
                            {
                                value.Min = Math.Min(value.Min ?? Double.MaxValue, currentVal);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Sum))
                            {
                                value.Sum = currentVal + (value.Sum ?? 0d);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Average))
                            {
                                value.Average = currentVal + (value.Average ?? 0d);
                            }
                        }
                        break;
                    case SortOptions.Double:
                        var doubles = FieldCache_Fields.DEFAULT.GetDoubles(indexReader, facet.AggregationField);
                        for (int index = 0; index < docsInQuery.Count; index++)
                        {
                            var doc = docsInQuery.Array[index];

                            var currentVal = doubles[doc - docBase];
                            if (facet.Aggregation.HasFlag(FacetAggregation.Max))
                            {
                                value.Max = Math.Max(value.Max ?? Double.MinValue, currentVal);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Min))
                            {
                                value.Min = Math.Min(value.Min ?? Double.MaxValue, currentVal);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Sum))
                            {
                                value.Sum = currentVal + (value.Sum ?? 0d);
                            }

                            if (facet.Aggregation.HasFlag(FacetAggregation.Average))
                            {
                                value.Average = currentVal + (value.Average ?? 0d);
                            }
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Cannot understand " + sortOptionsForFacet);
                }
            }

            private SortOptions GetSortOptionsForFacet(string field)
            {
                SortOptions value;
                if (indexDefinition.SortOptions.TryGetValue(field, out value) == false)
                {
                    if (field.EndsWith("_Range"))
                    {
                        var fieldWithNoRange = field.Substring(0, field.Length - "_Range".Length);
                        if (indexDefinition.SortOptions.TryGetValue(fieldWithNoRange, out value) == false)
                            value = SortOptions.None;
                    }
                    else
                    {
                        value = SortOptions.None;
                    }
                }
                return value;
            }

            private string TryTrimRangeSuffix(string fieldName)
            {
                return fieldName.EndsWith("_Range") ? fieldName.Substring(0, fieldName.Length - "_Range".Length) : fieldName;
            }

            public class FacetValueState
            {
                public HashSet<IndexSearcherHolder.StringCollectionValue> AlreadySeen;
                public HashSet<int> Docs;
                public Facet Facet;
                public ParsedRange Range;
            }

            private void UpdateFacetResults(Dictionary<string, Dictionary<string, FacetValue>> facetsByName)
            {
                foreach (var facet in Facets.Values)
                {
                    if (facet.Mode == FacetMode.Ranges)
                        continue;

                    var values = new List<FacetValue>();
                    List<string> allTerms;

                    int maxResults = Math.Min(PageSize ?? facet.MaxResults ?? Database.Configuration.MaxPageSize, Database.Configuration.MaxPageSize);
                    var groups = facetsByName.GetOrDefault(facet.DisplayName);

                    if (groups == null)
                        continue;

                    switch (facet.TermSortMode)
                    {
                        case FacetTermSortMode.ValueAsc:
                            allTerms = new List<string>(groups.OrderBy(x => x.Key).ThenBy(x => x.Value.Hits).Select(x => x.Key));
                            break;
                        case FacetTermSortMode.ValueDesc:
                            allTerms = new List<string>(groups.OrderByDescending(x => x.Key).ThenBy(x => x.Value.Hits).Select(x => x.Key));
                            break;
                        case FacetTermSortMode.HitsAsc:
                            allTerms = new List<string>(groups.OrderBy(x => x.Value.Hits).ThenBy(x => x.Key).Select(x => x.Key));
                            break;
                        case FacetTermSortMode.HitsDesc:
                            allTerms = new List<string>(groups.OrderByDescending(x => x.Value.Hits).ThenBy(x => x.Key).Select(x => x.Key));
                            break;
                        default:
                            throw new ArgumentException(string.Format("Could not understand '{0}'", facet.TermSortMode));
                    }

                    foreach (var term in allTerms.Skip(Start).TakeWhile(term => values.Count < maxResults))
                    {
                        var facetValue = groups.GetOrDefault(term);
                        values.Add(facetValue ?? new FacetValue { Range = term });
                    }

                    var previousHits = allTerms.Take(Start).Sum(allTerm =>
                    {
                        var facetValue = groups.GetOrDefault(allTerm);
                        return facetValue == null ? 0 : facetValue.Hits;
                    });

                    var key = string.IsNullOrWhiteSpace(facet.DisplayName) ? facet.Name : facet.DisplayName;

                    Results.Results[key] = new FacetResult
                    {
                        Values = values,
                        RemainingTermsCount = allTerms.Count - (Start + values.Count),
                        RemainingHits = groups.Values.Sum(x => x.Hits) - (previousHits + values.Sum(x => x.Hits)),
                    };

                    if (facet.IncludeRemainingTerms)
                        Results.Results[key].RemainingTerms = allTerms.Skip(Start + values.Count).ToList();
                }
            }
        }

        public class IntArraysPool : ILowMemoryHandler
        {
            public static IntArraysPool Instance = new IntArraysPool();

            private readonly ConcurrentDictionary<int, ObjectPool<int[]>> arraysPoolBySize = new ConcurrentDictionary<int, ObjectPool<int[]>>();
            private readonly TimeSensitiveStore<ObjectPool<int[]>> timeSensitiveStore = new TimeSensitiveStore<ObjectPool<int[]>>(TimeSpan.FromDays(1));

            private IntArraysPool()
            {

            }

            public int[] AllocateArray(int arraySize = 1024)
            {
                var roundedSize = GetRoundedSize(arraySize);
                var matchingQueue = arraysPoolBySize.GetOrAdd(roundedSize, x => new ObjectPool<int[]>(() => new int[roundedSize]));

                var allocatedArray = matchingQueue.Allocate();

                timeSensitiveStore.Seen(matchingQueue);

                return allocatedArray;
            }

            public void FreeArray(int[] returnedArray)
            {
                if (returnedArray.Length != GetRoundedSize(returnedArray.Length))
                {
                    throw new ArgumentException("Array size does not match current array size constraints");
                }


                var matchingQueue = arraysPoolBySize.GetOrAdd(returnedArray.Length, x => new ObjectPool<int[]>(() => new int[returnedArray.Length]));
                matchingQueue.Free(returnedArray);
            }

            private int GetRoundedSize(int size)
            {
                const int roundSize = 1024;
                if (size % roundSize == 0)
                {
                    return size;
                }

                return (size / roundSize + 1) * roundSize;
            }

            public void RunIdleOperations()
            {
                timeSensitiveStore.ForAllExpired(x =>
                {
                    var matchingQueue = arraysPoolBySize.FirstOrDefault(y => y.Value == x).Key;
                    if (matchingQueue != 0)
                    {
                        ObjectPool<int[]> removedQueue;
                        arraysPoolBySize.TryRemove(matchingQueue, out removedQueue);
                    }
                });
            }

            public LowMemoryHandlerStatistics HandleLowMemory()
            {
                RunIdleOperations();
                return new LowMemoryHandlerStatistics
                {
                    Name = "IntArraysPool",
                    Summary = "handle low memory called run idle operations for Faceted Query, and removes objects from array pool that weren't used in a long time"
                };
            }

            public void SoftMemoryRelease()
            {
            }

            public LowMemoryHandlerStatistics GetStats()
            {
                return new LowMemoryHandlerStatistics
                {
                    Name = "IntArraysPool",
                    //EstimatedUsedMemory = arraysPoolBySize.Select(x=>x.Value.)
                };
            }
        }

        public class ReaderFacetInfo
        {
            public IndexReader Reader;
            public int DocBase;
            // Here we store the _global document id_, if you need the 
            // reader document id, you must decrement with the DocBase
            public LinkedList<int[]> Matches;
            private int[] _current;
            private int _pos;
            public ArraySegment<int> Results;

            public ReaderFacetInfo()
            {
                _current = IntArraysPool.Instance.AllocateArray();
                Matches = new LinkedList<int[]>();
            }

            public void AddMatch(int doc)
            {
                if (_pos >= _current.Length)
                {
                    Matches.AddLast(_current);
                    _current = IntArraysPool.Instance.AllocateArray();
                    _pos = 0;
                }
                _current[_pos++] = doc + DocBase;
            }

            public void Complete()
            {
                var size = _pos;
                foreach (var match in Matches)
                {
                    size += match.Length;
                }

                var mergedAndSortedArray = IntArraysPool.Instance.AllocateArray(size);
                var curMergedArrayIndex = 0;
                foreach (var match in Matches)
                {
                    Array.Copy(match, 0, mergedAndSortedArray, curMergedArrayIndex, match.Length);
                    curMergedArrayIndex += match.Length;
                    IntArraysPool.Instance.FreeArray(match);
                }

                Array.Copy(_current, 0, mergedAndSortedArray, curMergedArrayIndex, _pos);
                IntArraysPool.Instance.FreeArray(_current);
                curMergedArrayIndex += _pos;
                _current = null;
                _pos = 0;

                Array.Sort(mergedAndSortedArray, 0, curMergedArrayIndex);
                Results = new ArraySegment<int>(mergedAndSortedArray, 0, curMergedArrayIndex);
            }
        }

        public class GatherAllCollectorByReader : Collector
        {
            private ReaderFacetInfo _current;
            public List<ReaderFacetInfo> Results = new List<ReaderFacetInfo>();


            public override void SetScorer(Scorer scorer)
            {
            }

            public override void Collect(int doc)
            {
                _current.AddMatch(doc);
            }

            public override void SetNextReader(IndexReader reader, int docBase)
            {
                _current = new ReaderFacetInfo
                {
                    DocBase = docBase,
                    Reader = reader,
                };
                Results.Add(_current);
            }

            public override bool AcceptsDocsOutOfOrder
            {
                get { return true; }
            }
        }
    }
}
