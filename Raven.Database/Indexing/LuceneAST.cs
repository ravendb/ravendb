using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Database.Indexing.LuceneIntegration;

namespace Raven.Database.Indexing
{
    public abstract class LuceneASTNodeBase
    {
        public abstract IEnumerable<LuceneASTNodeBase> Children { get; }

        public virtual Query ToQuery(RavenPerFieldAnalyzerWrapper analayzer, string fieldName, QueryOperator defaultOperator)
        {
            return null;
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

    public class FieldLuceneASTNode : LuceneASTNodeBase
    {
        public string FieldName { get; set; }
        public LuceneASTNodeBase Node { get; set; }
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield return Node; }
        }
        public override Query ToQuery(RavenPerFieldAnalyzerWrapper analayzer, string fieldName, QueryOperator defaultOperator)
        {
            return Node.ToQuery(analayzer, FieldName, defaultOperator);
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
/*            var splitList = matches.Where(x => x.Type == TermLuceneASTNode.TermType.UnQuoted && x.Term.Contains(',')).ToList();
            foreach (var splitNode in splitList)
            {
                matches.Remove(splitNode);
                var tokens = splitNode.Term.Split(commaSpliter,StringSplitOptions.RemoveEmptyEntries);
                foreach (var token in tokens)
                {
                    var node = new TermLuceneASTNode() { Term = token.Trim(), Type = TermLuceneASTNode.TermType.UnQuoted};
                    matches.Add(node);
                    //in case those unqouted split terms are actually not strings we need to detect this.
                    int i;
                    if (int.TryParse(node.Term, out i))
                    {
                        node.Type = TermLuceneASTNode.TermType.Int;
                        continue;
                    }
                    float f;
                    if (float.TryParse(node.Term, out f))
                    {
                        node.Type = TermLuceneASTNode.TermType.Float;
                        continue;
                    }
                    DateTime dt;
                    if (DateTime.TryParse(node.Term, out dt))
                    {
                        node.Type = TermLuceneASTNode.TermType.DateTime;
                        continue;
                    }
                    if (node.Term.StartsWith("[[") && node.Term.EndsWith("]]"))
                    {
                        node.Type = TermLuceneASTNode.TermType.UnAnalized;
                    }
                    
                }
            }*/
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
        public override Query ToQuery(RavenPerFieldAnalyzerWrapper analayzer, string fieldName, QueryOperator defaultOperator)
        {
            return new TermsMatchQuery(FieldName, Matches.Select(x => x.Term).ToList());
        }

        public override string ToString()
        {
            var sb = new StringBuilder(GetPrefixString()).Append("@").Append(MethodName).Append('<')
                .Append(FieldName).Append('>').Append(":(").Append(string.Join(" ,", Matches.Select(x => x.Term))).Append(")");
            return sb.ToString();
        }
        private static char[] commaSpliter = new char[] { ',' };
    }
    public class TermLuceneASTNode : LuceneASTNodeBase
    {
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield break; }
        }
        public override Query ToQuery(RavenPerFieldAnalyzerWrapper analayzer, string fieldName, QueryOperator defaultOperator)
        {
            switch (Type)
            {
                case TermType.Quoted:
                    //removing the quotes from the term
                    return new TermQuery(new Term(fieldName,Term.Substring(1,Term.Length-2)));
                    break;
                case TermType.UnQuoted:
                    return new TermQuery(new Term(fieldName, Term));
                    break;
                case TermType.Float:
                    return new TermQuery(new Term(fieldName, Term));
                    break;
                case TermType.DateTime:
                    return new TermQuery(new Term(fieldName, Term));
                    break;
                case TermType.Int:
                    return new TermQuery(new Term(fieldName, Term));
                    break;
                case TermType.UnAnalized:
                    return new TermQuery(new Term(fieldName, Term.Substring(2, Term.Length - 4)));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
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
            DateTime,
            Int,
            UnAnalized
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
        public override Query ToQuery(RavenPerFieldAnalyzerWrapper analayzer, string fieldName, QueryOperator defaultOperator)
        {
            return new TermRangeQuery(fieldName, RangeMin.Term, RangeMax.Term, InclusiveMin, InclusiveMax);
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
        public override Query ToQuery(RavenPerFieldAnalyzerWrapper analayzer, string fieldName, QueryOperator defaultOperator)
        {
            var query = new BooleanQuery();
            switch (Op)
            {
                case Operator.AND:
                    query.Add(LeftNode.ToQuery(analayzer,fieldName,defaultOperator),Occur.MUST);
                    query.Add(RightNode.ToQuery(analayzer, fieldName, defaultOperator), Occur.MUST);
                    break;
                case Operator.OR:
                    query.Add(LeftNode.ToQuery(analayzer,fieldName,defaultOperator),Occur.SHOULD);
                    query.Add(RightNode.ToQuery(analayzer, fieldName, defaultOperator), Occur.SHOULD);
                    break;
                case Operator.NOT:
                    query.Add(LeftNode.ToQuery(analayzer, fieldName, defaultOperator), Occur.MUST_NOT);
                    break;
                case Operator.Implicit:
                    switch (defaultOperator)
                    {
                        case QueryOperator.Or:
                            query.Add(LeftNode.ToQuery(analayzer,fieldName,defaultOperator),Occur.SHOULD);
                    query.Add(RightNode.ToQuery(analayzer, fieldName, defaultOperator), Occur.SHOULD);
                            break;
                        case QueryOperator.And:
                            query.Add(LeftNode.ToQuery(analayzer,fieldName,defaultOperator),Occur.MUST);
                            query.Add(RightNode.ToQuery(analayzer, fieldName, defaultOperator), Occur.MUST);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("defaultOperator");
                    }
                    break;
                case Operator.INTERSECT:
                    query.Add(LeftNode.ToQuery(analayzer,fieldName,defaultOperator),Occur.MUST);
                    query.Add(RightNode.ToQuery(analayzer, fieldName, defaultOperator), Occur.MUST);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return query;
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
        public override Query ToQuery(RavenPerFieldAnalyzerWrapper analayzer, string fieldName, QueryOperator defaultOperator)
        {
            var query = new BooleanQuery();
            query.Add(new BooleanClause(Node.ToQuery(analayzer,fieldName,defaultOperator),Occur.MUST));
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

