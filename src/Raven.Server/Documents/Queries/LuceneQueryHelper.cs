using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Server.Documents.Queries.Parser.Lucene;

namespace Raven.Server.Documents.Queries
{
    public class LuceneQueryHelper
    {
        public const string Asterisk = "*";

        public const char AsteriskChar = '*';

        public const string Null = "NULL";

        public static Query And(Query left, LucenePrefixOperator leftPrefix, Query right, LucenePrefixOperator rightPrefix)
        {
            return new BooleanQuery
            {
                { left, PrefixToOccurrence(leftPrefix, Occur.MUST) },
                { right, PrefixToOccurrence(rightPrefix, Occur.MUST) }
            };
        }

        public static Query Or(Query left, LucenePrefixOperator leftPrefix, Query right, LucenePrefixOperator rightPrefix)
        {
            return new BooleanQuery
            {
                { left, PrefixToOccurrence(leftPrefix, Occur.SHOULD) },
                { right, PrefixToOccurrence(rightPrefix, Occur.SHOULD) }
            };
        }

        public static Query Equal(string fieldName, LuceneFieldType fieldType, LuceneTermType termType, string value)
        {
            return CreateRange(fieldName, fieldType, value, termType, true, value, termType, true);
        }

        public static Query LessThan(string fieldName, LuceneFieldType fieldType, LuceneTermType termType, string value)
        {
            return CreateRange(fieldName, fieldType, Asterisk, LuceneTermType.WildCardTerm, false, value, termType, false);
        }

        public static Query LessThanOrEqual(string fieldName, LuceneFieldType fieldType, LuceneTermType termType, string value)
        {
            return CreateRange(fieldName, fieldType, Asterisk, LuceneTermType.WildCardTerm, false, value, termType, true);
        }

        public static Query GreaterThan(string fieldName, LuceneFieldType fieldType, LuceneTermType termType, string value)
        {
            return CreateRange(fieldName, fieldType, value, termType, false, Null, LuceneTermType.Null, true);
        }

        public static Query GreaterThanOrEqual(string fieldName, LuceneFieldType fieldType, LuceneTermType termType, string value)
        {
            return CreateRange(fieldName, fieldType, value, termType, true, Null, LuceneTermType.Null, true);
        }

        public static Query Between(string fieldName, LuceneFieldType fieldType, LuceneTermType termType, string fromValue, string toValue)
        {
            return CreateRange(fieldName, fieldType, fromValue, termType, true, toValue, termType, true);
        }

        public static IEnumerable<string> GetAnalyzedTerm(string fieldName, string term, LuceneTermType type, Analyzer analyzer)
        {
            switch (type)
            {
                case LuceneTermType.Quoted:
                case LuceneTermType.UnQuoted:
                    var tokenStream = analyzer.ReusableTokenStream(fieldName, new StringReader(term));
                    while (tokenStream.IncrementToken())
                    {
                        var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
                        yield return attribute.Term;
                    }
                    break;
                case LuceneTermType.QuotedWildcard:
                case LuceneTermType.WildCardTerm:
                case LuceneTermType.PrefixTerm:
                    yield return GetWildcardTerm(fieldName, term, type, analyzer).Text;
                    break;
                case LuceneTermType.Float:
                case LuceneTermType.Double:
                case LuceneTermType.Hex:
                case LuceneTermType.DateTime:
                case LuceneTermType.Int:
                case LuceneTermType.Long:
                case LuceneTermType.Null:
                    yield return term;
                    break;
                case LuceneTermType.UnAnalyzed:
                    yield return term.Substring(2, term.Length - 4);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public static Query Term(string fieldName, string term, LuceneTermType type, Analyzer analyzer, float? boost = null, float? similarity = null)
        {
            if (boost.HasValue == false)
                boost = 1;

            //Look into changing the grammar to better handle quoted/unquoted non-analyzed terms
            if (type == LuceneTermType.UnAnalyzed)
            {
                var originalLength = term.Length;
                var quoted = term[2] == '\"' && term[originalLength - 3] == '\"';
                var start = quoted ? 3 : 2;
                var length = quoted ? originalLength - 6 : originalLength - 4;
                return new TermQuery(new Term(fieldName, term.Substring(start, length))) { Boost = boost.Value };
            }

            switch (type)
            {
                case LuceneTermType.Float:
                case LuceneTermType.Hex:
                case LuceneTermType.Double:
                case LuceneTermType.DateTime:
                case LuceneTermType.Int:
                case LuceneTermType.Long:
                    return new TermQuery(new Term(fieldName, term)) { Boost = boost.Value };
            }

            if (type == LuceneTermType.QuotedWildcard)
            {
                var res = AnalyzedWildCardQueries(fieldName, term, type, analyzer);
                res.Boost = boost.Value;
                return res;
            }

            if (type == LuceneTermType.WildCardTerm)
            {
                var res = AnalyzedWildCardQueries(fieldName, term, type, analyzer);
                res.Boost = boost.Value;
                return res;
            }

            var tokenStream = analyzer.ReusableTokenStream(fieldName, new StringReader(term));
            var terms = new List<string>();

            while (tokenStream.IncrementToken())
            {
                var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
                terms.Add(attribute.Term);
            }

            if (type == LuceneTermType.PrefixTerm)
            {
                if (terms.Count != 0)
                {
                    var first = terms.First();
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

            if (terms.Count == 0)
                return null;

            if (type == LuceneTermType.Quoted)
            {
                /*if (!string.IsNullOrEmpty(Proximity))
                {
                    var pq = new PhraseQuery() { Boost = boost };
                    pq.Add(new Term(configuration.FieldName, Term));
                    pq.Slop = int.Parse(Proximity);
                    return pq;
                }*/
                if (terms.Count == 1)
                {
                    return new TermQuery(new Term(fieldName, terms.First())) { Boost = boost.Value };
                }
                var pq = new PhraseQuery { Boost = boost.Value };
                foreach (var t in terms)
                {
                    pq.Add(new Term(fieldName, t));
                }
                return pq;
                //return new TermQuery(new Term(configuration.FieldName, Term.Substring(1, Term.Length - 2))){Boost = boost};
            }

            //This is probably wrong, need to check what happens with analyzed unquoted terms.
            if (type == LuceneTermType.UnQuoted && similarity.HasValue)
            {
                return new FuzzyQuery(new Term(fieldName, terms.FirstOrDefault()), similarity.Value, 0) { Boost = boost.Value };
            }

            if (terms.Count == 1)
            {
                return new TermQuery(new Term(fieldName, terms[0])) { Boost = boost.Value };
            }

            var phrase = new PhraseQuery { Boost = boost.Value };
            foreach (var t in terms)
            {
                phrase.Add(new Term(fieldName, t));
            }

            return phrase;
        }


        private static Term GetWildcardTerm(string fieldName, string term, LuceneTermType type, Analyzer analyzer)
        {
            var quoted = type == LuceneTermType.QuotedWildcard;
            var reader = new StringReader(quoted ? term.Substring(1, term.Length - 2) : term);
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
                var firstTerm = terms.First();
                if (term.StartsWith(Asterisk) && !firstTerm.StartsWith(Asterisk)) sb.Append(Asterisk);
                sb.Append(firstTerm);
                if (term.EndsWith(Asterisk) && !firstTerm.EndsWith(Asterisk)) sb.Append(Asterisk);
                var res = sb.ToString();
                expectedLength = (quoted ? 2 : 0) + res.Length;
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
            expectedLength = analyzedTermString.Length + (quoted ? 2 : 0);
            Debug.Assert(expectedLength == term.Length,
                @"if analyzer changes length of term and removes wildcards after processing it, 
there is no way to know where to put the wildcard character back after the analysis. 
This edge-case has a very slim chance of happening, but still we should not ignore it completely.");

            return new Term(fieldName, analyzedTermString);
        }

        private static Query AnalyzedWildCardQueries(string fieldName, string term, LuceneTermType type, Analyzer analyzer)
        {
            return new WildcardQuery(GetWildcardTerm(fieldName, term, type, analyzer));
        }

        private static Query CreateRange(string fieldName, LuceneFieldType fieldType, string minValue, LuceneTermType minValueType, bool inclusiveMin, string maxValue, LuceneTermType maxValueType, bool inclusiveMax)
        {
            switch (fieldType)
            {
                case LuceneFieldType.String:
                    var minTermIsNullOrStar = minValueType == LuceneTermType.Null || minValue.Equals(Asterisk);
                    var maxTermIsNullOrStar = maxValueType == LuceneTermType.Null || maxValue.Equals(Asterisk);
                    if (minTermIsNullOrStar && maxTermIsNullOrStar)
                        return new WildcardQuery(new Term(fieldName, Asterisk));

                    return new TermRangeQuery(fieldName, minTermIsNullOrStar ? null : minValue, maxTermIsNullOrStar ? null : maxValue, inclusiveMin, inclusiveMax);
                case LuceneFieldType.Long:
                    var (lOverrideMin, lOverrideMax) = OverrideInclusiveForKnownNumericRange(minValue, minValueType, maxValue, maxValueType);

                    long longMin;
                    if (lOverrideMin)
                    {
                        longMin = long.MinValue;
                        inclusiveMin = true;
                    }
                    else
                    {
                        longMin = ParseTermToLong(minValue, minValueType);
                    }

                    long longMax;
                    if (lOverrideMax)
                    {
                        longMax = long.MaxValue;
                        inclusiveMax = true;
                    }
                    else
                    {
                        longMax = ParseTermToLong(maxValue, maxValueType);
                    }

                    return NumericRangeQuery.NewLongRange(fieldName, 4, longMin, longMax, inclusiveMin, inclusiveMax);
                case LuceneFieldType.Double:
                    var (dOverrideMin, dOverrideMax) = OverrideInclusiveForKnownNumericRange(minValue, minValueType, maxValue, maxValueType);

                    double doubleMin;
                    if (dOverrideMin)
                    {
                        doubleMin = double.MinValue;
                        inclusiveMin = true;
                    }
                    else
                    {
                        doubleMin = double.Parse(minValue);
                    }

                    double doubleMax;
                    if (dOverrideMax)
                    {
                        doubleMax = double.MaxValue;
                        inclusiveMax = true;
                    }
                    else
                    {
                        doubleMax = double.Parse(maxValue);
                    }

                    return NumericRangeQuery.NewDoubleRange(fieldName, 4, doubleMin, doubleMax, inclusiveMin, inclusiveMax);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long ParseTermToLong(string term, LuceneTermType type)
        {
            return type == LuceneTermType.Hex // TODO [ppekrol] is this needed?
                ? long.Parse(term.Substring(2), NumberStyles.HexNumber)
                : long.Parse(term);
        }

        /// <summary>
        /// For numeric values { NUll TO <number/> } should be [ <min-value/> TO <number/>} but not for string values.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static (bool OverrideMin, bool OverrideMax) OverrideInclusiveForKnownNumericRange(string minTerm, LuceneTermType minType, string maxTerm, LuceneTermType maxType)
        {
            bool overrideMax = false, overrideMin = false;
            if (maxType == LuceneTermType.Null || maxTerm == Asterisk)
            {
                overrideMax = true;
            }

            if (minType == LuceneTermType.Null || minTerm == Asterisk)
            {
                overrideMin = true;
            }

            return (overrideMin, overrideMax);
        }

        private static Occur PrefixToOccurrence(LucenePrefixOperator prefix, Occur defaultOccurrence)
        {
            switch (prefix)
            {
                case LucenePrefixOperator.None:
                    return defaultOccurrence;
                case LucenePrefixOperator.Plus:
                    return Occur.MUST;
                case LucenePrefixOperator.Minus:
                    return Occur.MUST_NOT;
                default:
                    throw new ArgumentOutOfRangeException("Unknown query prefix " + prefix);
            }
        }
    }
}