using Raven.Server.Documents.Queries.Parser;

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
    }
}
