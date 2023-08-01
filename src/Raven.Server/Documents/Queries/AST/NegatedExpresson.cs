namespace Raven.Server.Documents.Queries.AST
{
    public sealed class NegatedExpression : QueryExpression
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

        public override string GetTextWithAlias(IndexQueryServerSide parent)
        {
            return $"not {Expression.GetTextWithAlias(parent)}";
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is NegatedExpression ne))
                return false;

            return ne.Expression.Equals(Expression);
        }
    }
}
