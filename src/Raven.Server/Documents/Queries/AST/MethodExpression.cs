using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
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

        public override string ToString()
        {
            return Name + "(" + string.Join(", ", Arguments.Select(x => x.ToString())) + ")";
        }
    }
}
