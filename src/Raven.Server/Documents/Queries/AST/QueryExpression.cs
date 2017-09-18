using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Queries.Parser
{
    public abstract class QueryExpression
    {
        public ExpressionType Type;
    }
}
