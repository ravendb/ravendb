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
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries.LuceneIntegration;

namespace Raven.Server.Documents.Queries.Parse
{
    public class LuceneASTQueryConfiguration
    {
        public Analyzer Analayzer { get; set; }
        public FieldName FieldName { get; set; }
        public QueryOperator DefaultOperator { get; set; }
    }

    public class FieldName
    {
        public FieldName(string field, FieldType type = FieldType.String)
        {
            Field = field;
            Type = type;
        }
        public string Field { get; set; }
        public FieldType Type { get; set; }
        public override string ToString()
        {
            return Field;
        }

        public enum FieldType
        {
            String,
            Long,
            Double
        }
    }

    public abstract class LuceneASTNodeBase
    {
        public abstract IEnumerable<LuceneASTNodeBase> Children { get; }

        public abstract Query ToQuery(LuceneASTQueryConfiguration configuration);

        protected static string Asterisk = "*";

        public virtual void AddQueryToBooleanQuery(BooleanQuery b, LuceneASTQueryConfiguration configuration, Occur o)
        {
            var query = ToQuery(configuration);
            if(query != null)
                b.Add(query, o);
        }

        public virtual Query ToGroupFieldQuery(LuceneASTQueryConfiguration configuration)
        {
            return ToQuery(configuration);
        }

        public PrefixOperator Prefix { get; set; }

        protected string GetPrefixString()
        {
            switch (Prefix)
            {
                case PrefixOperator.None:
                    return string.Empty;
                case PrefixOperator.Plus:
                    return "+";
                case PrefixOperator.Minus:
                    return "-";
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public enum PrefixOperator
        {
            None,
            Plus,
            Minus

        };
    }

    public class AllDocumentsLuceneASTNode : LuceneASTNodeBase
    {
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield break; }
        }
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            return new MatchAllDocsQuery();
        }

        public override string ToString()
        {
            return GetPrefixString()+ "*:*";
        }
    }
    public class FieldLuceneASTNode : LuceneASTNodeBase
    {
        public FieldName FieldName { get; set; }
        public LuceneASTNodeBase Node { get; set; }
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield return Node; }
        }
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            configuration.FieldName = FieldName;
            var res = Node.ToGroupFieldQuery(configuration);
            return res;
        }
        public override string ToString()
        {
            return string.Format("{0}{1}:{2}",GetPrefixString(), FieldName, Node);
        }
    }
    
    public class MethodLuceneASTNode : LuceneASTNodeBase
    {        
        public MethodLuceneASTNode(string rawMethodStr, List<TermLuceneASTNode> matches)
        {            
            var fieldStartPos = rawMethodStr.IndexOf('<');
            MethodName = rawMethodStr.Substring(1, fieldStartPos-1);
            var fieldEndPos = rawMethodStr.IndexOf('>');
            FieldName = rawMethodStr.Substring(fieldStartPos + 1, fieldEndPos - fieldStartPos-1);
            Matches = matches;
        }

        public MethodLuceneASTNode(string rawMethodStr, TermLuceneASTNode match) : this(rawMethodStr, new List<TermLuceneASTNode>() { match }) { }
        public string MethodName { get; set; }
        public string FieldName { get; set; }
        public List<TermLuceneASTNode> Matches { get; set; }
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { return Matches; }
        }
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            configuration.FieldName = new FieldName (FieldName);
            var matchList = new List<string>();
            foreach (var match in Matches)
            {
                matchList.AddRange(match.GetAnalyzedTerm(configuration));
            }
            return new TermsMatchQuery(FieldName, matchList);
        }

        public override string ToString()
        {
            var sb = new StringBuilder(GetPrefixString()).Append("@").Append(MethodName).Append('<')
                .Append(FieldName).Append('>').Append(":(").Append(string.Join(" ,", Matches.Select(x => x.Term))).Append(")");
            return sb.ToString();
        }
    }
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
                    var tokenStream = configuration.Analayzer.ReusableTokenStream(configuration.FieldName.Field, new StringReader(Term));
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
            var qouted = Type == TermType.QuotedWildcard;
            var reader = new StringReader(qouted ? Term.Substring(1, Term.Length - 2) : Term);
            var tokenStream = configuration.Analayzer.ReusableTokenStream(configuration.FieldName.Field, reader);
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
                expectedLength = (qouted ? 2 : 0) + res.Length;
                Debug.Assert(expectedLength  == Term.Length,
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
            expectedLength = analyzedTermString.Length + (qouted ? 2 : 0);
            Debug.Assert(expectedLength == Term.Length,
@"if analyzer changes length of term and removes wildcards after processing it, 
there is no way to know where to put the wildcard character back after the analysis. 
This edge-case has a very slim chance of happening, but still we should not ignore it completely.");

            return new Term(configuration.FieldName.Field, analyzedTermString);
        }

        private Query AnalyzedWildCardQueries(LuceneASTQueryConfiguration configuration)
        {
            return new WildcardQuery(GetWildcardTerm(configuration));
        }

        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            var boost = string.IsNullOrEmpty(Boost) ? 1 : float.Parse(Boost);
            //Look into changing the grammer to better handle qouted/unqouted unanlyzed terms
            if (Type == TermType.UnAnalyzed)
            {
                var originalLength = Term.Length;
                var qouted = Term[2] == '\"' && Term[originalLength - 3] == '\"';
                var start = qouted ? 3 : 2;
                var length = qouted ? originalLength - 6 : originalLength - 4;
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

            var tokenStream = configuration.Analayzer.ReusableTokenStream(configuration.FieldName.Field, new StringReader(Term));
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
                var removeStar = Term.Substring(0, Term.Length-1);
                var booleanQuery = new BooleanQuery
                {
                    Clauses =
                    {
                        new BooleanClause(new PrefixQuery(new Term(configuration.FieldName.Field, removeStar )), Occur.SHOULD),
                        new BooleanClause(new PrefixQuery(new Term(configuration.FieldName.Field, removeStar.ToLowerInvariant())), Occur.SHOULD)
                    }
                    , Boost = boost
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
                    return new TermQuery(new Term(configuration.FieldName.Field,terms.First())){Boost = boost};
                }
                var pq = new PhraseQuery() { Boost = boost };
                foreach (var term in terms)
                {
                    pq.Add(new Term(configuration.FieldName.Field,term));
                }
                return pq;
                //return new TermQuery(new Term(configuration.FieldName, Term.Substring(1, Term.Length - 2))){Boost = boost};
            }
            //This is probably wrong, need to check what happens with analyzed unqouted terms.
            if (Type == TermType.UnQuoted && !string.IsNullOrEmpty(Similarity))
            {
                var similarity = float.Parse(Similarity);

                return new FuzzyQuery(new Term(configuration.FieldName.Field, terms.FirstOrDefault()), similarity, 0) { Boost = boost };
            }
            if (terms.Count == 1)
            {
                return new TermQuery(new Term(configuration.FieldName.Field, terms.First())) {Boost = boost};
            }
            var phrase = new PhraseQuery() { Boost = boost };
            foreach (var term in terms)
            {
                phrase.Add(new Term(configuration.FieldName.Field,term));
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
            var boost = string.IsNullOrEmpty(Boost)? string.Empty : "^" + Boost;
            var proximity = string.IsNullOrEmpty(Proximity)? string.Empty : "~" + Proximity;
            var similarity = string.IsNullOrEmpty(Similarity) ? string.Empty : "~" + Similarity;
            return String.Format("{0}{1}{2}{3}{4}", prefix, Term, boost, proximity, similarity);
        }

        public void SetPostfixOperators(PostfixModifiers modifiers)
        {
            Boost = modifiers.Boost;
            Proximity = modifiers.Proximity;
            Similarity = modifiers.Similerity;

        }
    }

    public class RangeLuceneASTNode : LuceneASTNodeBase
    {
        private bool _maxIsNull;
        private bool _minIsNull;        
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield break; }
        }
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            switch (configuration.FieldName.Type)
            {
                case FieldName.FieldType.String:
                    var minTermIsNullOrStar = RangeMin.Type == TermLuceneASTNode.TermType.Null || RangeMin.Term.Equals(Asterisk);
                    var maxTermIsNullOrStar = RangeMax.Type == TermLuceneASTNode.TermType.Null || RangeMax.Term.Equals(Asterisk);
                    if (minTermIsNullOrStar && maxTermIsNullOrStar)
                    {
                        return new WildcardQuery(new Term(configuration.FieldName.Field, Asterisk));
                    }
                    return new TermRangeQuery(configuration.FieldName.Field,
                                minTermIsNullOrStar ? null : RangeMin.Term,
                                maxTermIsNullOrStar ? null : RangeMax.Term,
                                InclusiveMin, InclusiveMax);
                case FieldName.FieldType.Long:
                    OverideInclusiveForKnownNumericRange();
                    var longMin = _minIsNull ? long.MinValue : ParseTermToLong(RangeMin);
                    var longMax = _maxIsNull ? long.MaxValue : ParseTermToLong(RangeMax);
                    return NumericRangeQuery.NewLongRange(configuration.FieldName.Field, 4, longMin, longMax, InclusiveMin, InclusiveMax);
                case FieldName.FieldType.Double:
                    OverideInclusiveForKnownNumericRange();
                    var doubleMin = _minIsNull ? double.MinValue : double.Parse(RangeMin.Term);
                    var doubleMax = _maxIsNull ? double.MaxValue : double.Parse(RangeMax.Term);
                    return NumericRangeQuery.NewDoubleRange(configuration.FieldName.Field, 4, doubleMin, doubleMax, InclusiveMin, InclusiveMax);
                default:
                    throw new ArgumentOutOfRangeException();
            }                                  
        }

        private long? ParseTermToLong(TermLuceneASTNode rangeMin)
        {
            return rangeMin.Type == TermLuceneASTNode.TermType.Hex
                ? long.Parse(rangeMin.Term.Substring(2), NumberStyles.HexNumber)
                : long.Parse(rangeMin.Term);
        }


        /// <summary>
        /// For numeric values { NUll TO <number/> } should be [ <min-value/> TO <number/>} but not for string values.
        /// </summary>
        private void OverideInclusiveForKnownNumericRange()
        {
            if (RangeMax.Type == TermLuceneASTNode.TermType.Null || RangeMax.Term == Asterisk)
            {
                _maxIsNull = true;
                InclusiveMax = true;
            }
            if (RangeMin.Type == TermLuceneASTNode.TermType.Null || RangeMin.Term == Asterisk)
            {
                _minIsNull = true;
                InclusiveMin = true;
            }
        }


        public TermLuceneASTNode RangeMin { get; set; }
        public TermLuceneASTNode RangeMax { get; set; }
        public bool InclusiveMin { get; set; }
        public bool InclusiveMax { get; set; }
        public override string ToString()
        {
            return string.Format("{0}{1} TO {2}{3}", InclusiveMin ? '[' : '{', RangeMin, RangeMax,
                InclusiveMax ? ']' : '}');
        }
    }

    public class OperatorLuceneASTNode : LuceneASTNodeBase
    {
        public OperatorLuceneASTNode(LuceneASTNodeBase leftNode, LuceneASTNodeBase rightNode, Operator op, bool isDefaultOperatorAnd)
        {
            var rightHandBooleanNode = rightNode as OperatorLuceneASTNode;
            //This basically say that if we have a nested boolean query and the child boolean is not an OR operation do nothing special.
            if (rightHandBooleanNode == null || 
                rightHandBooleanNode.Op == Operator.AND || 
                rightHandBooleanNode.Op == Operator.INTERSECT || 
                (rightHandBooleanNode.Op == Operator.Implicit && isDefaultOperatorAnd))
            {
                LeftNode = leftNode;
                RightNode = rightNode;
                Op = op;
            }
            //if we are in this case we are a boolean query with a child who is a boolean query with an OR operator .
            //we shall roll the nodes so (A) and (B or C) will become (A and b) or (C).
            else
            {
                LeftNode = new OperatorLuceneASTNode(leftNode, rightHandBooleanNode.LeftNode, op, isDefaultOperatorAnd);
                RightNode = rightHandBooleanNode.RightNode;
                Op = Operator.OR;
                //this should not be used but if it does a NRE exception will be thrown and it would be easy to locate it.
                rightHandBooleanNode.RightNode = null;
                rightHandBooleanNode.LeftNode = null;
            }
        }
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { 
                yield return LeftNode;
                if (RightNode != null) yield return RightNode;
            }
        }
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            var query = new BooleanQuery();
            switch (Op)
            {
                case Operator.AND:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(LeftNode, Occur.MUST));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(RightNode, Occur.MUST));
                    break;
                case Operator.OR:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(LeftNode, Occur.SHOULD));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(RightNode, Occur.SHOULD));
                    break;
                case Operator.Implicit:
                    switch (configuration.DefaultOperator)
                    {
                        case QueryOperator.Or:
                            LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(LeftNode, Occur.SHOULD));
                            RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(RightNode, Occur.SHOULD));
                            break;
                        case QueryOperator.And:
                            LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(LeftNode, Occur.MUST));
                            RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(RightNode, Occur.MUST));
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("defaultOperator");
                    }
                    break;
                case Operator.INTERSECT:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(LeftNode, Occur.MUST));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(RightNode, Occur.MUST));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return query;
        }

        private Occur PrefixToOccurance(LuceneASTNodeBase node, Occur defaultOccurance)
        {             
            switch (node.Prefix)
            {
                case PrefixOperator.None:
                    return defaultOccurance;
                case PrefixOperator.Plus:
                    return Occur.MUST;
                case PrefixOperator.Minus:
                    return Occur.MUST_NOT;
                default:
                    throw new ArgumentOutOfRangeException("Unknown query prefix " + node.Prefix);
            }
        }

        public override void AddQueryToBooleanQuery(BooleanQuery query, LuceneASTQueryConfiguration configuration, Occur o)
        {
            switch (Op)
            {
                case Operator.AND:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(LeftNode, Occur.MUST));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(RightNode, Occur.MUST));
                    break;
                case Operator.OR:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(LeftNode, Occur.SHOULD));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(RightNode, Occur.SHOULD));
                    break;
                case Operator.Implicit:
                    switch (configuration.DefaultOperator)
                    {
                        case QueryOperator.Or:
                            LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(LeftNode,Occur.SHOULD));
                            RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(RightNode, Occur.SHOULD));
                            break;
                        case QueryOperator.And:
                            LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(LeftNode, Occur.MUST));
                            RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(RightNode, Occur.MUST));
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("defaultOperator");
                    }
                    break;
                case Operator.INTERSECT:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(LeftNode, Occur.MUST));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurance(RightNode, Occur.MUST));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public enum Operator
        {
            AND,
            OR,
            Implicit,
            INTERSECT
        }
        public LuceneASTNodeBase RightNode { get; set; }

        public LuceneASTNodeBase LeftNode { get; set; }

        public Operator Op { get; set; }
        public override string ToString()
        {
            if (Op == Operator.Implicit)
            {
                return LeftNode + " " + RightNode;
            }
            return LeftNode + " " + Op + " " + RightNode;
        }
    }

    public class ParenthesisLuceneASTNode : LuceneASTNodeBase
    {
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield return Node; }
        }
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            var query = new BooleanQuery();
            var occur = configuration.DefaultOperator == QueryOperator.And ? Occur.MUST : Occur.SHOULD;
            //if the node is boolean query than it is going to ignore this value.
            Node.AddQueryToBooleanQuery(query, configuration, occur); 
            query.Boost = GetBoost();
            return query;
        }

        public override Query ToGroupFieldQuery(LuceneASTQueryConfiguration configuration)
        {
            var query = Node.ToQuery(configuration);
            if (query == null)
                return null;
            query.Boost = GetBoost();
            return query;
        }

        public LuceneASTNodeBase Node { get; set; }
        public string Boost { get; set; }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append('(').Append(Node).Append(')').Append(string.IsNullOrEmpty(Boost)?string.Empty:string.Format("^{0}",Boost));
            return sb.ToString();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float GetBoost()
        {
            return  Boost == null ? 1 : float.Parse(Boost);
        }

    }
    public class PostfixModifiers
    {
        public string Boost { get; set; }
        public string Similerity { get; set; }
        public string Proximity { get; set; }
    }
}

