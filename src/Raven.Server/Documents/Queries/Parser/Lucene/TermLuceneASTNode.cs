using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class TermLuceneASTNode : LuceneASTNodeBase
    {
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield break; }
        }

        public IEnumerable<string> GetAnalyzedTerm(LuceneASTQueryConfiguration configuration)
        {
            switch (Type)
            {
                case TermType.Quoted:
                case TermType.UnQuoted:
                    var tokenStream = configuration.Analyzer.ReusableTokenStream(configuration.FieldName.Field, new StringReader(Term));
                    while (tokenStream.IncrementToken())
                    {
                        var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
                        yield return attribute.Term;
                    }
                    break;
                case TermType.QuotedWildcard:
                case TermType.WildCardTerm:
                case TermType.PrefixTerm:
                    yield return GetWildcardTerm(configuration).Text;
                    break;
                case TermType.Float:
                case TermType.Double:
                case TermType.Hex:
                case TermType.DateTime:
                case TermType.Int:
                case TermType.Long:
                case TermType.Null:
                    yield return Term;
                    break;
                case TermType.UnAnalyzed:
                    yield return Term.Substring(2, Term.Length - 4);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private Term GetWildcardTerm(LuceneASTQueryConfiguration configuration)
        {
            var quoted = Type == TermType.QuotedWildcard;
            var reader = new StringReader(quoted ? Term.Substring(1, Term.Length - 2) : Term);
            var tokenStream = configuration.Analyzer.ReusableTokenStream(configuration.FieldName.Field, reader);
            var terms = new List<string>();
            while (tokenStream.IncrementToken())
            {
                var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
                terms.Add(attribute.Term);
            }

            if (terms.Count == 0)
            {
                return new Term(configuration.FieldName.Field, Term);
            }

            var sb = new StringBuilder();
            int expectedLength;
            if (terms.Count == 1)
            {
                var firstTerm = terms.First();
                if (Term.StartsWith(Asterisk) && !firstTerm.StartsWith(Asterisk)) sb.Append('*');
                sb.Append(firstTerm);
                if (Term.EndsWith(Asterisk) && !firstTerm.EndsWith(Asterisk)) sb.Append('*');
                var res = sb.ToString();
                expectedLength = (quoted ? 2 : 0) + res.Length;
                Debug.Assert(expectedLength == Term.Length,
                    @"if analyzer changes length of term and removes wildcards after processing it, 
there is no way to know where to put the wildcard character back after the analysis. 
This edge-case has a very slim chance of happening, but still we should not ignore it completely.");
                return new Term(configuration.FieldName.Field, res);
            }

            foreach (var currentTerm in terms)
            {
                if (sb.Length < Term.Length)
                {
                    var c = Term[sb.Length];
                    if (c == '?' || c == '*')
                    {
                        sb.Append(c);
                    }
                }
                sb.Append(currentTerm);
            }

            var analyzedTermString = sb.ToString();
            expectedLength = analyzedTermString.Length + (quoted ? 2 : 0);
            Debug.Assert(expectedLength == Term.Length,
                @"if analyzer changes length of term and removes wildcards after processing it, 
there is no way to know where to put the wildcard character back after the analysis. 
This edge-case has a very slim chance of happening, but still we should not ignore it completely.");

            return new Term(configuration.FieldName.Field, analyzedTermString);
        }

        private global::Lucene.Net.Search.Query AnalyzedWildCardQueries(LuceneASTQueryConfiguration configuration)
        {
            return new WildcardQuery(GetWildcardTerm(configuration));
        }

        public override global::Lucene.Net.Search.Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            var boost = string.IsNullOrEmpty(Boost) ? 1 : float.Parse(Boost);
            //Look into changing the grammar to better handle quoted/unquoted non-analyzed terms
            if (Type == TermType.UnAnalyzed)
            {
                var originalLength = Term.Length;
                var quoted = Term[2] == '\"' && Term[originalLength - 3] == '\"';
                var start = quoted ? 3 : 2;
                var length = quoted ? originalLength - 6 : originalLength - 4;
                return new TermQuery(new Term(configuration.FieldName.Field, Term.Substring(start, length))) { Boost = boost };
            }
            switch (Type)
            {
                case TermType.Float:
                case TermType.Hex:
                case TermType.Double:
                case TermType.DateTime:
                case TermType.Int:
                case TermType.Long:
                    return new TermQuery(new Term(configuration.FieldName.Field, Term)) { Boost = boost };
            }

            if (Type == TermType.QuotedWildcard)
            {
                var res = AnalyzedWildCardQueries(configuration);
                res.Boost = boost;
                return res;
            }

            if (Type == TermType.WildCardTerm)
            {
                var res = AnalyzedWildCardQueries(configuration);
                res.Boost = boost;
                return res;
            }

            var tokenStream = configuration.Analyzer.ReusableTokenStream(configuration.FieldName.Field, new StringReader(Term));
            var terms = new List<string>();

            while (tokenStream.IncrementToken())
            {
                var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
                terms.Add(attribute.Term);
            }
            if (Type == TermType.PrefixTerm)
            {
                if (terms.Count != 0)
                {
                    var first = terms.First();
                    var actualTerm = first[first.Length - 1] == '*' ? first.Substring(0, first.Length - 1) : first;
                    return new PrefixQuery(new Term(configuration.FieldName.Field, actualTerm)) { Boost = boost };
                }
                // if the term that we are trying to prefix has been removed entirely by the analyzer, then we are going
                // to cheat a bit, and check for both the term in as specified and the term in lower case format so we can
                // find it regardless of casing
                var removeStar = Term.Substring(0, Term.Length - 1);
                var booleanQuery = new BooleanQuery
                {
                    Clauses =
                    {
                        new BooleanClause(new PrefixQuery(new Term(configuration.FieldName.Field, removeStar )), Occur.SHOULD),
                        new BooleanClause(new PrefixQuery(new Term(configuration.FieldName.Field, removeStar.ToLowerInvariant())), Occur.SHOULD)
                    }
                    ,
                    Boost = boost
                };
                return booleanQuery;
            }
            if (terms.Count == 0) return null;

            if (Type == TermType.Quoted)
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
                    return new TermQuery(new Term(configuration.FieldName.Field, terms.First())) { Boost = boost };
                }
                var pq = new PhraseQuery() { Boost = boost };
                foreach (var term in terms)
                {
                    pq.Add(new Term(configuration.FieldName.Field, term));
                }
                return pq;
                //return new TermQuery(new Term(configuration.FieldName, Term.Substring(1, Term.Length - 2))){Boost = boost};
            }
            //This is probably wrong, need to check what happens with analyzed unquoted terms.
            if (Type == TermType.UnQuoted && !string.IsNullOrEmpty(Similarity))
            {
                var similarity = float.Parse(Similarity);

                return new FuzzyQuery(new Term(configuration.FieldName.Field, terms.FirstOrDefault()), similarity, 0) { Boost = boost };
            }
            if (terms.Count == 1)
            {
                return new TermQuery(new Term(configuration.FieldName.Field, terms.First())) { Boost = boost };
            }
            var phrase = new PhraseQuery() { Boost = boost };
            foreach (var term in terms)
            {
                phrase.Add(new Term(configuration.FieldName.Field, term));
            }
            return phrase;
        }

        public string Boost { get; set; }
        public string Similarity { get; set; }
        public string Proximity { get; set; }
        public string Term { get; set; }
        public TermType Type { get; set; }

        public enum TermType
        {
            Quoted,
            QuotedWildcard,
            UnQuoted,
            Float,
            Double,
            DateTime,
            Int,
            Long,
            UnAnalyzed,
            Null,
            WildCardTerm,
            PrefixTerm,
            Hex
        }



        public override string ToString()
        {
            var prefix = Prefix == PrefixOperator.Plus ? "+" : Prefix == PrefixOperator.Minus ? "-" : "";
            var boost = string.IsNullOrEmpty(Boost) ? string.Empty : "^" + Boost;
            var proximity = string.IsNullOrEmpty(Proximity) ? string.Empty : "~" + Proximity;
            var similarity = string.IsNullOrEmpty(Similarity) ? string.Empty : "~" + Similarity;
            return $"{prefix}{Term}{boost}{proximity}{similarity}";
        }

        public void SetPostfixOperators(PostfixModifiers modifiers)
        {
            Boost = modifiers.Boost;
            Proximity = modifiers.Proximity;
            Similarity = modifiers.Similarity;

        }
    }
}