using System;
using System.Collections.Generic;
using System.DirectoryServices.ActiveDirectory;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Database.Indexing.LuceneIntegration;

namespace Raven.Database.Indexing
{
	public class LuceneASTQueryConfiguration
	{
		public RavenPerFieldAnalyzerWrapper Analayzer { get; set; }
		public string FieldName { get; set; }
		public QueryOperator DefaultOperator { get; set; }
	}

    public abstract class LuceneASTNodeBase
    {
        public abstract IEnumerable<LuceneASTNodeBase> Children { get; }

	    public abstract Query ToQuery(LuceneASTQueryConfiguration configuration);

		public virtual void AddQueryToBooleanQuery(BooleanQuery b, LuceneASTQueryConfiguration configuration, Occur o = Occur.MUST)
	    {
			b.Add(ToQuery(configuration), o);
	    }

        public PrefixOperator Prefix { get; set; }

        protected string GetPrefixString()
        {
            switch (Prefix)
            {
                case PrefixOperator.None:
                    return string.Empty;
                    break;
                case PrefixOperator.Plus:
                    return "+";
                    break;
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
        public string FieldName { get; set; }
        public LuceneASTNodeBase Node { get; set; }
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield return Node; }
        }
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
	        configuration.FieldName = FieldName;
	        var res = Node.ToQuery(configuration);
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
            return new TermsMatchQuery(FieldName, Matches.Select(x => x.Term).ToList());
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
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
	        var boost = string.IsNullOrEmpty(Boost) ? 1 : float.Parse(Boost);
			if (Type == TermType.UnAnalyzed)
			{
				return new TermQuery(new Term(configuration.FieldName, Term.Substring(2, Term.Length - 4))) { Boost = boost };
			}
	        switch (Type)
	        {
		        case TermType.Float:
		        case TermType.Double:
		        case TermType.DateTime:
		        case TermType.Int:
		        case TermType.Long:
					return new TermQuery(new Term(configuration.FieldName, Term)) { Boost = boost };
	        }
	        var tokenStream = configuration.Analayzer.ReusableTokenStream(configuration.FieldName, new StringReader(Term));
	        List<string> terms = new List<string>();
			while (tokenStream.IncrementToken())
			{
				var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
				terms.Add(attribute.Term);
			}
	        if (Type == TermType.Quoted)
	        {
		        /*if (!string.IsNullOrEmpty(Proximity))
		        {
					var pq = new PhraseQuery() { Boost = boost };
					pq.Add(new Term(configuration.FieldName, Term));
			        pq.Slop = int.Parse(Proximity);
			        return pq;
		        }*/
				var pq = new PhraseQuery() { Boost = boost };
				foreach (var term in terms)
		        {
					pq.Add(new Term(configuration.FieldName,term));
		        }
		        return pq;
				//return new TermQuery(new Term(configuration.FieldName, Term.Substring(1, Term.Length - 2))){Boost = boost};
	        }

			if (Type == TermType.WildCardTerm)
			{
				return new WildcardQuery(new Term(configuration.FieldName, terms.FirstOrDefault())) { Boost = boost };
			}
	        if (Type == TermType.UnQuoted && !string.IsNullOrEmpty(Similarity))
	        {
				var similarity = string.IsNullOrEmpty(Similarity) ? (float)0.5 : float.Parse(Similarity);

				return new FuzzyQuery(new Term(configuration.FieldName, terms.FirstOrDefault()), similarity, 0) { Boost = boost };
	        }
			return new TermQuery(new Term(configuration.FieldName, terms.FirstOrDefault())) { Boost = boost };
        }

        public PrefixOperator Prefix { get; set; }
        public string Boost { get; set; }
        public string Similarity { get; set; }
        public string Proximity { get; set; }
        public string Term { get; set; }

        public TermType Type { get; set; }

        public enum TermType
        {
            Quoted,
            UnQuoted,
            Float,
			Double,
            DateTime,
            Int,
			Long,
            UnAnalyzed,
			Null,
			WildCardTerm
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
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield break; }
        }
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
			if (RangeMin.Type == TermLuceneASTNode.TermType.Float || RangeMax.Type == TermLuceneASTNode.TermType.Float)
            {
                //Need to handle NULL values...
				var min = (RangeMin.Type == TermLuceneASTNode.TermType.Null) ? float.MinValue : float.Parse(RangeMin.Term);
				var max = (RangeMax.Type == TermLuceneASTNode.TermType.Null) ? float.MaxValue : float.Parse(RangeMax.Term);
				return NumericRangeQuery.NewFloatRange(configuration.FieldName, 4, min, max, InclusiveMin, InclusiveMax);
            }
			if (RangeMin.Type == TermLuceneASTNode.TermType.Double || RangeMax.Type == TermLuceneASTNode.TermType.Double)
			{
				//Need to handle NULL values...
				var min = (RangeMin.Type == TermLuceneASTNode.TermType.Null) ? double.MinValue : double.Parse(RangeMin.Term);
				var max = (RangeMax.Type == TermLuceneASTNode.TermType.Null) ? double.MaxValue : double.Parse(RangeMax.Term);
				return NumericRangeQuery.NewDoubleRange(configuration.FieldName, 4, min, max, InclusiveMin, InclusiveMax);
			}
			if (RangeMin.Type == TermLuceneASTNode.TermType.Int || RangeMax.Type == TermLuceneASTNode.TermType.Int)
            {
				var intMin = (RangeMin.Type == TermLuceneASTNode.TermType.Null) ? int.MinValue : int.Parse(RangeMin.Term);
				var intMax = (RangeMax.Type == TermLuceneASTNode.TermType.Null) ? int.MaxValue : int.Parse(RangeMax.Term);
				return NumericRangeQuery.NewLongRange(configuration.FieldName, 4, intMin, intMax, InclusiveMin, InclusiveMax);
            }
			if (RangeMin.Type == TermLuceneASTNode.TermType.Long || RangeMax.Type == TermLuceneASTNode.TermType.Long)
			{
				var longMin = (RangeMin.Type == TermLuceneASTNode.TermType.Null) ? long.MinValue : long.Parse(RangeMin.Term);
				var longMax = (RangeMax.Type == TermLuceneASTNode.TermType.Null) ? long.MaxValue : long.Parse(RangeMax.Term);
				return NumericRangeQuery.NewLongRange(configuration.FieldName, 4, longMin, longMax, InclusiveMin, InclusiveMax);
			}
	        if (RangeMin.Type == TermLuceneASTNode.TermType.Null && RangeMax.Type == TermLuceneASTNode.TermType.Null)
	        {
				return new WildcardQuery(new Term(configuration.FieldName, "*"));
	        }
			return new TermRangeQuery(configuration.FieldName, RangeMin.Term, RangeMax.Term, InclusiveMin, InclusiveMax);
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
        public OperatorLuceneASTNode(LuceneASTNodeBase leftNode, LuceneASTNodeBase rightNode, Operator op)
        {
            LeftNode = leftNode;
            RightNode = rightNode;
            Operator parsedOperator;
            Op = op;
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
		            LeftNode.AddQueryToBooleanQuery(query, configuration);
					RightNode.AddQueryToBooleanQuery(query, configuration, RightNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
                    break;
                case Operator.OR:
					LeftNode.AddQueryToBooleanQuery(query, configuration, Occur.SHOULD);
					RightNode.AddQueryToBooleanQuery(query, configuration, Occur.SHOULD);
                    break;
                case Operator.NOT:
                    query.Add(LeftNode.ToQuery(configuration), Occur.MUST_NOT);
                    break;
                case Operator.Implicit:
					switch (configuration.DefaultOperator)
                    {
                        case QueryOperator.Or:
                            LeftNode.AddQueryToBooleanQuery(query, configuration, Occur.SHOULD);
							RightNode.AddQueryToBooleanQuery(query, configuration, Occur.SHOULD);
                            break;
                        case QueryOperator.And:
							LeftNode.AddQueryToBooleanQuery(query, configuration);
							RightNode.AddQueryToBooleanQuery(query, configuration, RightNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("defaultOperator");
                    }
                    break;
                case Operator.INTERSECT:
		            LeftNode.AddQueryToBooleanQuery(query, configuration);
					RightNode.AddQueryToBooleanQuery(query, configuration, RightNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return query;
        }

		public override void AddQueryToBooleanQuery(BooleanQuery query, LuceneASTQueryConfiguration configuration, Occur o = Occur.MUST)
	    {
			switch (Op)
			{
				case Operator.AND:
					LeftNode.AddQueryToBooleanQuery(query, configuration);
					RightNode.AddQueryToBooleanQuery(query, configuration, RightNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
					break;
				case Operator.OR:
					LeftNode.AddQueryToBooleanQuery(query, configuration, Occur.SHOULD);
					RightNode.AddQueryToBooleanQuery(query, configuration, Occur.SHOULD);
					break;
				case Operator.NOT:
					query.Add(LeftNode.ToQuery(configuration), Occur.MUST_NOT);
					break;
				case Operator.Implicit:
					switch (configuration.DefaultOperator)
					{
						case QueryOperator.Or:
							LeftNode.AddQueryToBooleanQuery(query, configuration, Occur.SHOULD);
							RightNode.AddQueryToBooleanQuery(query, configuration, Occur.SHOULD);
							break;
						case QueryOperator.And:
							LeftNode.AddQueryToBooleanQuery(query, configuration);
							RightNode.AddQueryToBooleanQuery(query, configuration, RightNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
							break;
						default:
							throw new ArgumentOutOfRangeException("defaultOperator");
					}
					break;
				case Operator.INTERSECT:
					LeftNode.AddQueryToBooleanQuery(query, configuration);
					RightNode.AddQueryToBooleanQuery(query, configuration, RightNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
					break;
				default:
					throw new ArgumentOutOfRangeException();
			}
	    }

	    public enum Operator
        {
            AND,
            OR,
            NOT,
            Implicit,
            INTERSECT
        }
        public LuceneASTNodeBase RightNode { get; set; }

        public LuceneASTNodeBase LeftNode { get; set; }

        public Operator Op { get; set; }
        public override string ToString()
        {
            if (Op == Operator.NOT)
            {
                return "NOT " + LeftNode.ToString();
            }
            if (Op == Operator.Implicit)
            {
                return LeftNode + " " + RightNode;
            }
            return LeftNode + " " + Op + " " + RightNode;
        }
    }

    public class ParenthesistLuceneASTNode : LuceneASTNodeBase
    {
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield return Node; }
        }
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            var query = new BooleanQuery();
            query.Add(new BooleanClause(Node.ToQuery(configuration),Occur.MUST));
	        query.Boost = Boost == null ? 1 : float.Parse(Boost);
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
    }
    public class PostfixModifiers
    {
        public string Boost { get; set; }
        public string Similerity { get; set; }
        public string Proximity { get; set; }
    }
}

