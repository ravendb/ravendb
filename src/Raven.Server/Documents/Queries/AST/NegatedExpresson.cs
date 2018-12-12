using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Queries.AST
{
    public class NegatedExpression : QueryExpression
    {
        public QueryExpression Expression;

        public NegatedExpression(QueryExpression expr)
        {
            Expression = expr;
            Type = ExpressionType.Negated;
        }

        public override string ToString()
        {
            return "Not ( " + Expression + ")";
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return $"not {Expression.GetText(parent)}";
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is NegatedExpression ne))
                return false;

            return ne.Expression.Equals(Expression);
        }
    }
}
