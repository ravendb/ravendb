using System.Collections.Generic;
using Sparrow;

namespace Raven.Server.Documents.Queries.Parser
{
    public class MethodExpression : QueryExpression
    {
        public StringSegment Name;
        public List<QueryExpression> Arguments;

        public MethodExpression(StringSegment name, List<QueryExpression> arguments)
        {
            Name = name;
            Arguments = arguments;
            Type = ExpressionType.Method;
        }
    }
}
