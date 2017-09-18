namespace Raven.Server.Documents.Queries.Parser
{
    public class TrueExpression : QueryExpression
    {
        public TrueExpression()
        {
            Type = ExpressionType.True;
        }
    }
}
