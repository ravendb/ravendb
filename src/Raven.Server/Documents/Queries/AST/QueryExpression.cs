using System;

namespace Raven.Server.Documents.Queries.AST
{
    public abstract class QueryExpression
    {
        public ExpressionType Type;

        public abstract override String ToString();
    }
}
