using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Lucene.Net.Search;
using Raven.Client.Documents.Queries;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class ParenthesisLuceneASTNode : LuceneASTNodeBase
    {
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield return Node; }
        }
        public override global::Lucene.Net.Search.Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            var query = new BooleanQuery();
            var occur = configuration.DefaultOperator == QueryOperator.And ? Occur.MUST : Occur.SHOULD;
            //if the node is boolean query than it is going to ignore this value.
            Node.AddQueryToBooleanQuery(query, configuration, occur);
            query.Boost = GetBoost();
            return query;
        }

        public override global::Lucene.Net.Search.Query ToGroupFieldQuery(LuceneASTQueryConfiguration configuration)
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
            sb.Append('(').Append(Node).Append(')').Append(string.IsNullOrEmpty(Boost) ? string.Empty : string.Format("^{0}", Boost));
            return sb.ToString();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float GetBoost()
        {
            return Boost == null ? 1 : float.Parse(Boost);
        }

    }
}