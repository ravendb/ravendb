using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Client;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers.Collation;
using Raven.Server.Documents.Queries.LuceneIntegration;
using Sparrow.Json;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.Queries
{
    public sealed class LuceneQueryHelper
    {
        public const string Asterisk = "*";

        public const char AsteriskChar = '*';

        public const string Null = "NULL";

        public static Query Equal(string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return Term(fieldName, value, termType, exact: exact);
        }

        public static Query Equal(Index index, string fieldName, long value)
        {
            return CreateRange(index, fieldName, value, true, value, true);
        }

        public static Query Equal(Index index, string fieldName, double value)
        {
            return CreateRange( index, fieldName, value, true, value, true);
        }

        public static Query NotEqual(string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return new BooleanQuery
            {
                { new MatchAllDocsQuery(), Occur.MUST },
                { Term(fieldName, value, termType, exact: exact), Occur.MUST_NOT }
            };
        }

        public static Query NotEqual(Index index, string fieldName, long value)
        {
            return new BooleanQuery
            {
                { new MatchAllDocsQuery(), Occur.MUST },
                { CreateRange(index, fieldName, value, true, value, true), Occur.MUST_NOT }
            };
        }

        public static Query NotEqual(Index index, string fieldName, double value)
        {
            return new BooleanQuery
            {
                { new MatchAllDocsQuery(), Occur.MUST },
                { CreateRange(index, fieldName, value, true, value, true), Occur.MUST_NOT }
            };
        }

        public static Query LessThan(Index index, string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return CreateRange(index, fieldName, Asterisk, LuceneTermType.WildCard, false, value, termType, false, exact);
        }

        public static Query LessThan(Index index, string fieldName, long value)
        {
            return CreateRange(index, fieldName, long.MinValue, true, value, false);
        }

        public static Query LessThan(Index index, string fieldName, double value)
        {
            return CreateRange(index, fieldName, double.MinValue, true, value, false);
        }

        public static Query LessThanOrEqual(Index index, string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return CreateRange(index, fieldName, Asterisk, LuceneTermType.WildCard, false, value, termType, true, exact);
        }

        public static Query LessThanOrEqual(Index index, string fieldName, long value)
        {
            return CreateRange(index, fieldName, long.MinValue, true, value, true);
        }

        public static Query LessThanOrEqual(Index index, string fieldName, double value)
        {
            return CreateRange(index, fieldName, double.MinValue, true, value, true);
        }

        public static Query GreaterThan(Index index, string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return CreateRange(index, fieldName, value, termType, false, Null, LuceneTermType.Null, true, exact);
        }

        public static Query GreaterThan(Index index, string fieldName, long value)
        {
            return CreateRange(index, fieldName, value, false, long.MaxValue, true);
        }

        public static Query GreaterThan(Index index, string fieldName, double value)
        {
            return CreateRange(index,fieldName, value, false, double.MaxValue, true);
        }

        public static Query GreaterThanOrEqual(Raven.Server.Documents.Indexes.Index  index, string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return CreateRange(index, fieldName, value, termType, true, Null, LuceneTermType.Null, true, exact);
        }

        public static Query GreaterThanOrEqual(Index index, string fieldName, long value)
        {
            return CreateRange(index, fieldName, value, true, long.MaxValue, true);
        }

        public static Query GreaterThanOrEqual(Index index, string fieldName, double value)
        {
            return CreateRange(index, fieldName, value, true, double.MaxValue, true);
        }

        public static Query Between(Raven.Server.Documents.Indexes.Index index, string fieldName, global::Raven.Server.Documents.Queries.LuceneTermType termType, string fromValue, bool fromInclusive, string toValue, bool toInclusive, bool exact)
        {
            return CreateRange(index, fieldName, fromValue, termType, fromInclusive, toValue, termType, toInclusive, exact);
        }

        public static Query Between(Index index, string fieldName, long fromValue, bool fromInclusive, long toValue, bool toInclusive)
        {
            return CreateRange(index, fieldName, fromValue, fromInclusive, toValue, toInclusive);
        }

        public static Query Between(Index index, string fieldName, double fromValue, bool fromInclusive, double toValue, bool toInclusive)
        {
            return CreateRange(index, fieldName, fromValue, fromInclusive, toValue, toInclusive);
        }

        public static Query Term(string fieldName, string term, LuceneTermType type, float? boost = null, float? similarity = null, bool exact = false)
        {
            if (boost.HasValue == false)
                boost = 1;

            if (type == LuceneTermType.Double || type == LuceneTermType.Long)
                return new TermQuery(new Term(fieldName, term)) { Boost = boost.Value };

            term = GetTermValue(term, type, exact);

            if (type == LuceneTermType.WildCard)
            {
                return new WildcardQuery(new Term(fieldName, term))
                {
                    Boost = boost.Value
                };
            }

            if (type == LuceneTermType.Prefix)
            {
                var actualTerm = term[term.Length - 1] == AsteriskChar ? term.Substring(0, term.Length - 1) : term;
                return new PrefixQuery(new Term(fieldName, actualTerm)) { Boost = boost.Value };
            }

            return new TermQuery(new Term(fieldName, term)) { Boost = boost.Value };
        }

        private static ThreadLocal<LowerCaseKeywordAnalyzer> WildcardAnalyzer = new ThreadLocal<LowerCaseKeywordAnalyzer>(() => new());
        public static Query AnalyzedTerm(string fieldName, string term, LuceneTermType type, Analyzer analyzer, float? boost = null, float? similarity = null)
        {
            if (type != LuceneTermType.String && type != LuceneTermType.Prefix && type != LuceneTermType.WildCard)
                throw new InvalidOperationException("Analyzed terms can be only created from string values.");

            if (boost.HasValue == false)
                boost = 1;

            if (type is LuceneTermType.Prefix or LuceneTermType.WildCard)
            {
                var analyzerToUse = analyzer switch
                {
                    LuceneRavenPerFieldAnalyzerWrapper wrapper => wrapper.GetAnalyzer(fieldName),
                    _ => analyzer
                };
                analyzer = analyzerToUse switch
                {
                    KeywordAnalyzer keywordAnalyzer => keywordAnalyzer,
                    // here we force a lower case keyword analyzer to ensure proper behavior
                    // https://ayende.com/blog/191841-B/understanding-query-processing-and-wildcards-in-ravendb
                    RavenStandardAnalyzer or NGramAnalyzer => WildcardAnalyzer.Value,
                    LowerCaseKeywordAnalyzer or CollationAnalyzer => analyzer,
                    // if the user has a custom analyzer, we'll use that, and they can deal with any surprises
                    // in wildcard queries
                    _ => analyzer
                };
                Debug.Assert(analyzer != null);
            }

            var tokenStream = analyzer.ReusableTokenStream(fieldName, new StringReader(term));
            var terms = new List<string>();

            while (tokenStream.IncrementToken())
            {
                var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
                terms.Add(attribute.Term);
            }

            switch (type)
            {
                case LuceneTermType.Prefix:
                {
                    return new PrefixQuery(new Term(fieldName, terms[0].TrimEnd(AsteriskChar)))
                    {
                        Boost = boost.Value
                    };
                }
                case LuceneTermType.WildCard:
                    return new WildcardQuery(new Term(fieldName, terms[0]))
                    {
                        Boost = boost.Value
                    };
            }

            if (terms.Count == 1)
            {
                return new TermQuery(new Term(fieldName, terms[0]))
                {
                    Boost = boost.Value
                };
            }

            var pq = new PhraseQuery
            {
                Boost = boost.Value
            };

            foreach (var t in terms)
                pq.Add(new Term(fieldName, t));

            return pq;
        }

        public static unsafe string GetTermValue(string value, LuceneTermType type, bool exact)
        {
            switch (type)
            {
                case LuceneTermType.Double:
                case LuceneTermType.Long:
                    // In the case of a nullable numeric value, we have to match it via NULL_VALUE.
                    return value ?? Constants.Documents.Indexing.Fields.NullValue;
                default:
                    {
                        if (value == null)
                            return Constants.Documents.Indexing.Fields.NullValue;

                        if (value == string.Empty)
                            return Constants.Documents.Indexing.Fields.EmptyString;

                        if (exact)
                            return value;

                        fixed (char* pValue = value)
                        {
                            var result = LazyStringParser.TryParseDateTime(pValue, value.Length, out DateTime _, out DateTimeOffset _, properlyParseThreeDigitsMilliseconds: true);
                            switch (result)
                            {
                                case LazyStringParser.Result.DateTime:
                                case LazyStringParser.Result.DateTimeOffset:
                                    return value;
                                default:
                                    return value.ToLowerInvariant();
                            }
                        }
                    }
            }
        }

        private static Query CreateRange(Index index, string fieldName, string minValue, LuceneTermType minValueType, bool inclusiveMin, string maxValue,
            LuceneTermType maxValueType, bool inclusiveMax, bool exact)
        {
            var minTermIsNullOrStar = minValueType == LuceneTermType.Null || minValue.Equals(Asterisk);
            var maxTermIsNullOrStar = maxValueType == LuceneTermType.Null || maxValue.Equals(Asterisk);
            if (minTermIsNullOrStar && maxTermIsNullOrStar)
                return new WildcardQuery(new Term(fieldName, Asterisk));

            var range = new TermRangeQuery(fieldName, minTermIsNullOrStar ? null : GetTermValue(minValue, minValueType, exact), maxTermIsNullOrStar ? null : GetTermValue(maxValue, maxValueType, exact), inclusiveMin, inclusiveMax);
            return MaybeCacheQuery(index, range);
        }

        private static Query CreateRange(Index index, string fieldName, long minValue, bool inclusiveMin, long maxValue, bool inclusiveMax)
        {
            var query= NumericRangeQuery.NewLongRange(fieldName, 4, minValue, maxValue, inclusiveMin, inclusiveMax);
            return MaybeCacheQuery(index, query);
        }

        private static Query CreateRange(Index index, string fieldName, double minValue, bool inclusiveMin, double maxValue, bool inclusiveMax)
        {
            var query = NumericRangeQuery.NewDoubleRange(fieldName, 4, minValue, maxValue, inclusiveMin, inclusiveMax);
            
            return MaybeCacheQuery(index, query);
        }

        private static Query MaybeCacheQuery(Index index, Query query)
        {
            if (index.Configuration.QueryClauseCacheDisabled)
                return query;
            return new CachingQuery(query, index, query.ToString());
        }
    }
}
