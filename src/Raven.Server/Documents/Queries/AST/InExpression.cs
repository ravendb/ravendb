using System.Collections.Generic;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Queries.Parser
{
    public class InExpression : QueryExpression
    {
        public bool All;
        public QueryExpression Source;
        public List<QueryExpression> Values;

        public InExpression(QueryExpression source, List<QueryExpression> values, bool all)
        {
            All = all;
            Source = source;
            Values = values;
            Type = ExpressionType.In;
        }
    }
}
