using Microsoft.Extensions.Primitives;

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

        public override string GetText(IndexQueryServerSide parent)
        {
            if (parent != null && Value == ValueTokenType.Parameter)
            {
                if (parent.QueryParameters.TryGet(Token, out object o))
                    return o?.ToString();
            }
            return Token.Value;
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is ValueExpression ve))
                return false;

            return Token == ve.Token && Value == ve.Value;
        }
    }
}
