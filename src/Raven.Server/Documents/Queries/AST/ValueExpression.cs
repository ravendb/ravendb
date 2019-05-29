using System;
using Sparrow;
using System.Globalization;
using Sparrow.Json;

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

        public object GetValue(BlittableJsonReaderObject queryParameters)
        {
            switch (Value)
            {
                case ValueTokenType.Parameter:
                    queryParameters.TryGetMember(Token, out var r);
                    return r;
                case ValueTokenType.Long:
                    return QueryBuilder.ParseInt64WithSeparators(Token.Value);
                case ValueTokenType.Double:
                    return double.Parse(Token.AsSpan(), NumberStyles.AllowThousands | NumberStyles.Float, CultureInfo.InvariantCulture);
                case ValueTokenType.String:
                    return Token;
                case ValueTokenType.True:
                    return QueryExpressionExtensions.True;
                case ValueTokenType.False:
                    return QueryExpressionExtensions.False;
                case ValueTokenType.Null:
                    return null;
                default:
                    throw new InvalidOperationException("Unknown ValueExpression value: " + Value);
            }
        }

    }
}
