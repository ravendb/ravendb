namespace Raven.Server.Documents.Queries.AST
{
    public class TrueExpression : QueryExpression
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

        public override bool Equals(QueryExpression other)
        {
            return other is TrueExpression;
        }
    }
}
