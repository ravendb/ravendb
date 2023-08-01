namespace Raven.Server.Documents.Queries.AST
{
    public sealed class TrueExpression : QueryExpression
    {
        public TrueExpression()
        {
            Type = ExpressionType.True;
        }

        public override string ToString()
        {
            return "true";
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return ToString();
        }

        public override string GetTextWithAlias(IndexQueryServerSide parent)
        {
            return GetText(parent);
        }

        public override bool Equals(QueryExpression other)
        {
            return other is TrueExpression;
        }
    }
}
