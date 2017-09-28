using Sparrow;

namespace Raven.Server.Documents.Queries.AST
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

        public override string ToString()
        {
            return Token + " (" + Value + ")";
        }

        public override string GetText()
        {
            return ToString();
        }
    }
}
