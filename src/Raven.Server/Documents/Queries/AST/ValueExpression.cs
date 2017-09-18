using Raven.Server.Documents.Queries.AST;
using Sparrow;

namespace Raven.Server.Documents.Queries.Parser
{
    public class ValueExpression : QueryExpression
    {
        public StringSegment Token;
        public ValueTokenType Value;

        public ValueExpression(StringSegment token, ValueTokenType type)
        {
            Token = token;
            Value = type;
            Type = ExpressionType.Value;
        }
        
        public ValueExpression(StringSegment token, ValueTokenType type, bool escapeChars)
        {
            Token = token;
            Value = type;
            Type = ExpressionType.Value;
        }
    }
}