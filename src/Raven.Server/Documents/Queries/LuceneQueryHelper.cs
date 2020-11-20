using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class LuceneQueryHelper
    {
        public const string Asterisk = "*";

        public const char AsteriskChar = '*';

        public const string Null = "NULL";

        public static Query Equal(string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return Term(fieldName, value, termType, exact: exact);
        }

        public static Query Equal(string fieldName, long value)
        {
            return CreateRange(fieldName, value, true, value, true);
        }

        public static Query Equal(string fieldName, double value)
        {
            return CreateRange(fieldName, value, true, value, true);
        }

        public static Query NotEqual(string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return new BooleanQuery
            {
                { new MatchAllDocsQuery(), Occur.MUST },
                { Term(fieldName, value, termType, exact: exact), Occur.MUST_NOT }
            };
        }

        public static Query NotEqual(string fieldName, long value)
        {
            return new BooleanQuery
            {
                { new MatchAllDocsQuery(), Occur.MUST },
                { CreateRange(fieldName, value, true, value, true), Occur.MUST_NOT }
            };
        }

        public static Query NotEqual(string fieldName, double value)
        {
            return new BooleanQuery
            {
                { new MatchAllDocsQuery(), Occur.MUST },
                { CreateRange(fieldName, value, true, value, true), Occur.MUST_NOT }
            };
        }

        public static Query LessThan(string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return CreateRange(fieldName, Asterisk, LuceneTermType.WildCard, false, value, termType, false, exact);
        }

        public static Query LessThan(string fieldName, long value)
        {
            return CreateRange(fieldName, long.MinValue, true, value, false);
        }

        public static Query LessThan(string fieldName, double value)
        {
            return CreateRange(fieldName, double.MinValue, true, value, false);
        }

        public static Query LessThanOrEqual(string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return CreateRange(fieldName, Asterisk, LuceneTermType.WildCard, false, value, termType, true, exact);
        }

        public static Query LessThanOrEqual(string fieldName, long value)
        {
            return CreateRange(fieldName, long.MinValue, true, value, true);
        }

        public static Query LessThanOrEqual(string fieldName, double value)
        {
            return CreateRange(fieldName, double.MinValue, true, value, true);
        }

        public static Query GreaterThan(string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return CreateRange(fieldName, value, termType, false, Null, LuceneTermType.Null, true, exact);
        }

        public static Query GreaterThan(string fieldName, long value)
        {
            return CreateRange(fieldName, value, false, long.MaxValue, true);
        }

        public static Query GreaterThan(string fieldName, double value)
        {
            return CreateRange(fieldName, value, false, double.MaxValue, true);
        }

        public static Query GreaterThanOrEqual(string fieldName, LuceneTermType termType, string value, bool exact)
        {
            return CreateRange(fieldName, value, termType, true, Null, LuceneTermType.Null, true, exact);
        }

        public static Query GreaterThanOrEqual(string fieldName, long value)
        {
            return CreateRange(fieldName, value, true, long.MaxValue, true);
        }

        public static Query GreaterThanOrEqual(string fieldName, double value)
        {
            return CreateRange(fieldName, value, true, double.MaxValue, true);
        }

        public static Query Between(string fieldName, LuceneTermType termType, string fromValue, string toValue, bool exact)
        {
            return CreateRange(fieldName, fromValue, termType, true, toValue, termType, true, exact);
        }

        public static Query Between(string fieldName, long fromValue, long toValue)
        {
            return CreateRange(fieldName, fromValue, true, toValue, true);
        }

        public static Query Between(string fieldName, double fromValue, double toValue)
        {
            return CreateRange(fieldName, fromValue, true, toValue, true);
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

        public static Query AnalyzedTerm(string fieldName, string term, LuceneTermType type, Analyzer analyzer, float? boost = null, float? similarity = null)
        {
            if (type != LuceneTermType.String && type != LuceneTermType.Prefix && type != LuceneTermType.WildCard)
                throw new InvalidOperationException("Analyzed terms can be only created from string values.");

            if (boost.HasValue == false)
                boost = 1;

            if (type == LuceneTermType.WildCard)
            {
                return new WildcardQuery(GetAnalyzedWildcardTerm(fieldName, term, analyzer))
                {
                    Boost = boost.Value
                };
            }

            var tokenStream = analyzer.ReusableTokenStream(fieldName, new StringReader(term));
            var terms = new List<string>();

            while (tokenStream.IncrementToken())
            {
                var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
                terms.Add(attribute.Term);
            }

            if (type == LuceneTermType.Prefix)
            {
                if (terms.Count != 0)
                {
                    var first = terms[0];
                    var actualTerm = first[first.Length - 1] == AsteriskChar ? first.Substring(0, first.Length - 1) : first;
                    return new PrefixQuery(new Term(fieldName, actualTerm)) { Boost = boost.Value };
                }
                // if the term that we are trying to prefix has been removed entirely by the analyzer, then we are going
                // to cheat a bit, and check for both the term in as specified and the term in lower case format so we can
                // find it regardless of casing
                var removeStar = term.Substring(0, term.Length - 1);
                var booleanQuery = new BooleanQuery
                {
                    Clauses =
                    {
                        new BooleanClause(new PrefixQuery(new Term(fieldName, removeStar)), Occur.SHOULD),
                        new BooleanClause(new PrefixQuery(new Term(fieldName, removeStar.ToLowerInvariant())), Occur.SHOULD)
                    },
                    Boost = boost.Value
                };
                return booleanQuery;
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
                    return value;
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
                            var result = LazyStringParser.TryParseDateTime(pValue, value.Length, out DateTime _, out DateTimeOffset _);
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

        private static Term GetAnalyzedWildcardTerm(string fieldName, string term, Analyzer analyzer)
        {
            var reader = new StringReader(term);
            var tokenStream = analyzer.ReusableTokenStream(fieldName, reader);
            var terms = new List<string>();
            while (tokenStream.IncrementToken())
            {
                var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
                terms.Add(attribute.Term);
            }

            if (terms.Count == 0)
            {
                return new Term(fieldName, term);
            }

            var sb = new StringBuilder();
            int expectedLength;
            if (terms.Count == 1)
            {
                var firstTerm = terms[0];
                if (term.StartsWith(Asterisk) && !firstTerm.StartsWith(Asterisk))
                    sb.Append(Asterisk);

                sb.Append(firstTerm);
                if (term.EndsWith(Asterisk) && !firstTerm.EndsWith(Asterisk))
                    sb.Append(Asterisk);

                var res = sb.ToString();
                expectedLength = res.Length;
                Debug.Assert(expectedLength == term.Length,
                    @"if analyzer changes length of term and removes wildcards after processing it,
there is no way to know where to put the wildcard character back after the analysis.
This edge-case has a very slim chance of happening, but still we should not ignore it completely.");
                return new Term(fieldName, res);
            }

            foreach (var currentTerm in terms)
            {
                if (sb.Length < term.Length)
                {
                    var c = term[sb.Length];
                    if (c == '?' || c == AsteriskChar)
                    {
                        sb.Append(c);
                    }
                }
                sb.Append(currentTerm);
            }

            var analyzedTermString = sb.ToString();
            expectedLength = analyzedTermString.Length;
            Debug.Assert(expectedLength == term.Length,
                @"if analyzer changes length of term and removes wildcards after processing it,
there is no way to know where to put the wildcard character back after the analysis.
This edge-case has a very slim chance of happening, but still we should not ignore it completely.");

            return new Term(fieldName, analyzedTermString);
        }


        private static Query CreateRange(string fieldName, string minValue, LuceneTermType minValueType, bool inclusiveMin, string maxValue, LuceneTermType maxValueType, bool inclusiveMax, bool exact)
        {
            var minTermIsNullOrStar = minValueType == LuceneTermType.Null || minValue.Equals(Asterisk);
            var maxTermIsNullOrStar = maxValueType == LuceneTermType.Null || maxValue.Equals(Asterisk);
            if (minTermIsNullOrStar && maxTermIsNullOrStar)
                return new WildcardQuery(new Term(fieldName, Asterisk));

            return new TermRangeQuery(fieldName, minTermIsNullOrStar ? null : GetTermValue(minValue, minValueType, exact), maxTermIsNullOrStar ? null : GetTermValue(maxValue, maxValueType, exact), inclusiveMin, inclusiveMax);
        }

        private static Query CreateRange(string fieldName, long minValue, bool inclusiveMin, long maxValue, bool inclusiveMax)
        {
            return NumericRangeQuery.NewLongRange(fieldName, 4, minValue, maxValue, inclusiveMin, inclusiveMax);
        }

        private static Query CreateRange(string fieldName, double minValue, bool inclusiveMin, double maxValue, bool inclusiveMax)
        {
            var query = NumericRangeQuery.NewDoubleRange(fieldName, 4, minValue, maxValue, inclusiveMin, inclusiveMax);
            
            return query;
        }
    }
}
