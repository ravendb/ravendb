using System;
using System.Collections.Generic;
using System.DirectoryServices.ActiveDirectory;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Database.Indexing.LuceneIntegration;
using Raven.Database.Util;

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
					var tokenStream = configuration.Analayzer.ReusableTokenStream(configuration.FieldName, new StringReader(Term));
					while (tokenStream.IncrementToken())
					{
						var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
						yield return attribute.Term;
					}
				    break;
				case TermType.WildCardTerm:
				case TermType.PrefixTerm:
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

		private Query AnalyzedWildCardQueries(LuceneASTQueryConfiguration configuration)
	    {
			var tokenStream = configuration.Analayzer.ReusableTokenStream(configuration.FieldName, new StringReader(Term));
			List<string> terms = new List<string>();
			while (tokenStream.IncrementToken())
			{
				var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
				terms.Add(attribute.Term);
			}
			if (terms.Count == 0)
			{
				return new WildcardQuery(new Term(configuration.FieldName, Term)); 
			}
			var sb = new StringBuilder();
			if (terms.Count == 1)
			{
				var term = terms.First();
				if (Term.StartsWith("*") && !term.StartsWith("*")) sb.Append('*');
				sb.Append(term);
				if (Term.EndsWith("*") && !term.EndsWith("*")) sb.Append('*');
				return new WildcardQuery(new Term(configuration.FieldName,sb.ToString()));
			}
			foreach (var term in terms)
			{
				if (sb.Length < Term.Length)
				{
					var c = Term[sb.Length];
					if (c == '?' || c == '*')
					{
						sb.Append(c);
					}
				}
				sb.Append(term);
			}
			return new WildcardQuery(new Term(configuration.FieldName, sb.ToString()));

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
				case TermType.Hex:
		        case TermType.Double:
		        case TermType.DateTime:
		        case TermType.Int:
		        case TermType.Long:
					return new TermQuery(new Term(configuration.FieldName, Term)) { Boost = boost };
	        }
			if (Type == TermType.WildCardTerm)
			{
				var res = AnalyzedWildCardQueries(configuration);
				res.Boost = boost;
				return res;
			}
	        var tokenStream = configuration.Analayzer.ReusableTokenStream(configuration.FieldName, new StringReader(Term));
	        List<string> terms = new List<string>();
			while (tokenStream.IncrementToken())
			{
				var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
				terms.Add(attribute.Term);
			}
			if (Type == TermType.PrefixTerm)
			{

				return new PrefixQuery(new Term(configuration.FieldName, terms.Count == 0 ? string.Empty : terms.First().Substring(0, terms.First().Length-1))) { Boost = boost };
			}
			if (terms.Count == 0) return new BooleanQuery();

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
			        return new TermQuery(new Term(configuration.FieldName,terms.First())){Boost = boost};
		        }
				var pq = new PhraseQuery() { Boost = boost };
				foreach (var term in terms)
		        {
					pq.Add(new Term(configuration.FieldName,term));
		        }
		        return pq;
				//return new TermQuery(new Term(configuration.FieldName, Term.Substring(1, Term.Length - 2))){Boost = boost};
	        }
			//This is probably wrong, need to check what happens with analyzed unqouted terms.
	        if (Type == TermType.UnQuoted && !string.IsNullOrEmpty(Similarity))
	        {
				var similarity = string.IsNullOrEmpty(Similarity) ? (float)0.5 : float.Parse(Similarity);

				return new FuzzyQuery(new Term(configuration.FieldName, terms.FirstOrDefault()), similarity, 0) { Boost = boost };
	        }
	        if (terms.Count == 1)
	        {
		        return new TermQuery(new Term(configuration.FieldName, terms.First())) {Boost = boost};
	        }
			var phrase = new PhraseQuery() { Boost = boost };
	        foreach (var term in terms)
	        {
		        phrase.Add(new Term(configuration.FieldName,term));
	        }
	        return phrase;
        }

        public PrefixOperator Prefix { get; set; }
        public string Boost { get; set; }
        public string Similarity { get; set; }
        public string Proximity { get; set; }
		private string term;
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
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield break; }
        }
        public override Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
			if (RangeMin.Type == TermLuceneASTNode.TermType.Float || RangeMax.Type == TermLuceneASTNode.TermType.Float)
            {
                //Need to handle NULL values...
				var min = (RangeMin.Type == TermLuceneASTNode.TermType.Null || RangeMin.Term == "*") ? float.MinValue : float.Parse(RangeMin.Term.Substring(2));
				var max = (RangeMax.Type == TermLuceneASTNode.TermType.Null || RangeMax.Term == "*") ? float.MaxValue : float.Parse(RangeMax.Term.Substring(2));
				return NumericRangeQuery.NewFloatRange(configuration.FieldName, 4, min, max, InclusiveMin, InclusiveMax);
            }
			if (RangeMin.Type == TermLuceneASTNode.TermType.Double || RangeMax.Type == TermLuceneASTNode.TermType.Double)
			{
				//numbers inside range without prefix are treated as strings.
				if (!RangeMin.Term.StartsWith("Dx") && !RangeMax.Term.StartsWith("Dx"))
					return new TermRangeQuery(configuration.FieldName, 
						RangeMin.Type == TermLuceneASTNode.TermType.Null?null:RangeMin.Term, 
						RangeMax.Type == TermLuceneASTNode.TermType.Null?null:RangeMax.Term, 
						InclusiveMin, InclusiveMax);
				var min = (RangeMin.Type == TermLuceneASTNode.TermType.Null || RangeMin.Term == "*") ? double.MinValue : double.Parse(RangeMin.Term.Substring(2));
				var max = (RangeMax.Type == TermLuceneASTNode.TermType.Null || RangeMax.Term == "*") ? double.MaxValue : double.Parse(RangeMax.Term.Substring(2));
				return NumericRangeQuery.NewDoubleRange(configuration.FieldName, 4, min, max, InclusiveMin, InclusiveMax);
			}
			if (RangeMin.Type == TermLuceneASTNode.TermType.Int || RangeMax.Type == TermLuceneASTNode.TermType.Int)
            {
				//numbers inside range without prefix are treated as strings.
				if (!RangeMin.Term.StartsWith("Ix") && !RangeMax.Term.StartsWith("Ix"))
					return new TermRangeQuery(configuration.FieldName,
						RangeMin.Type == TermLuceneASTNode.TermType.Null ? null : RangeMin.Term,
						RangeMax.Type == TermLuceneASTNode.TermType.Null ? null : RangeMax.Term,
						InclusiveMin, InclusiveMax);
				var intMin = (RangeMin.Type == TermLuceneASTNode.TermType.Null || RangeMin.Term == "*") ? int.MinValue : int.Parse(RangeMin.Term.Substring(2));
				var intMax = (RangeMax.Type == TermLuceneASTNode.TermType.Null || RangeMax.Term == "*") ? int.MaxValue : int.Parse(RangeMax.Term.Substring(2));
				return NumericRangeQuery.NewIntRange(configuration.FieldName, 4, intMin, intMax, InclusiveMin, InclusiveMax);
            }
			if (RangeMin.Type == TermLuceneASTNode.TermType.Long || RangeMax.Type == TermLuceneASTNode.TermType.Long)
			{
				var longMin = (RangeMin.Type == TermLuceneASTNode.TermType.Null || RangeMin.Term == "*") ? long.MinValue : long.Parse(RangeMin.Term.Substring(2));
				var longMax = (RangeMax.Type == TermLuceneASTNode.TermType.Null || RangeMax.Term == "*") ? long.MaxValue : long.Parse(RangeMax.Term.Substring(2));
				return NumericRangeQuery.NewLongRange(configuration.FieldName, 4, longMin, longMax, InclusiveMin, InclusiveMax);
			}
	        if (RangeMin.Type == TermLuceneASTNode.TermType.Hex || RangeMax.Type == TermLuceneASTNode.TermType.Hex)
	        {
				var longMin = (RangeMin.Type == TermLuceneASTNode.TermType.Null || RangeMin.Term == "*") ? long.MinValue : long.Parse(RangeMin.Term.Substring(2), NumberStyles.HexNumber);
				var longMax = (RangeMax.Type == TermLuceneASTNode.TermType.Null || RangeMax.Term == "*") ? long.MaxValue : long.Parse(RangeMax.Term.Substring(2), NumberStyles.HexNumber);
		        if (RangeMin.Type == TermLuceneASTNode.TermType.Hex && RangeMax.Type == TermLuceneASTNode.TermType.Hex)
		        {
			        if (longMax <= int.MaxValue)
			        {
						return NumericRangeQuery.NewIntRange(configuration.FieldName, 4, (int)longMin, (int)longMax, InclusiveMin, InclusiveMax);
			        }
					return NumericRangeQuery.NewLongRange(configuration.FieldName, 4, longMin, longMax, InclusiveMin, InclusiveMax);
		        }
				if (RangeMin.Type == TermLuceneASTNode.TermType.Hex && longMin <= int.MaxValue)
		        {
					return NumericRangeQuery.NewIntRange(configuration.FieldName, 4, (int)longMin, int.MaxValue, InclusiveMin, InclusiveMax);
		        }
				if (RangeMax.Type == TermLuceneASTNode.TermType.Hex && longMax <= int.MaxValue)
				{
					return NumericRangeQuery.NewIntRange(configuration.FieldName, 4, int.MinValue, (int)longMax, InclusiveMin, InclusiveMax);
				}
				return NumericRangeQuery.NewLongRange(configuration.FieldName, 4, longMin, longMax, InclusiveMin, InclusiveMax);
	        }
	        if (RangeMin.Type == TermLuceneASTNode.TermType.Null && RangeMax.Type == TermLuceneASTNode.TermType.Null)
	        {
				return new WildcardQuery(new Term(configuration.FieldName, "*"));
	        }
			return new TermRangeQuery(configuration.FieldName,
						RangeMin.Type == TermLuceneASTNode.TermType.Null ? null : RangeMin.Term,
						RangeMax.Type == TermLuceneASTNode.TermType.Null ? null : RangeMax.Term,
						InclusiveMin, InclusiveMax);
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
					LeftNode.AddQueryToBooleanQuery(query, configuration, LeftNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
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
							LeftNode.AddQueryToBooleanQuery(query, configuration, LeftNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
							RightNode.AddQueryToBooleanQuery(query, configuration, RightNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("defaultOperator");
                    }
                    break;
                case Operator.INTERSECT:
					LeftNode.AddQueryToBooleanQuery(query, configuration, LeftNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
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
					LeftNode.AddQueryToBooleanQuery(query, configuration, LeftNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
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
							LeftNode.AddQueryToBooleanQuery(query, configuration, LeftNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
							RightNode.AddQueryToBooleanQuery(query, configuration, RightNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
							break;
						default:
							throw new ArgumentOutOfRangeException("defaultOperator");
					}
					break;
				case Operator.INTERSECT:
					LeftNode.AddQueryToBooleanQuery(query, configuration, LeftNode.Prefix == PrefixOperator.Minus ? Occur.MUST_NOT : Occur.MUST);
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
			Node.AddQueryToBooleanQuery(query,configuration);
	        query.Boost = Boost == null ? 1 : float.Parse(Boost);
            return query;
        }

	    public override Query ToGroupFieldQuery(LuceneASTQueryConfiguration configuration)
	    {
		    return Node.ToQuery(configuration);
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

