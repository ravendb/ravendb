using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public abstract class WhereExpressionVisitor
    {
        private readonly string _queryText;

        protected WhereExpressionVisitor(string queryText)
        {
            _queryText = queryText;
        }

        public void Visit(QueryExpression expression, BlittableJsonReaderObject parameters)
        {
            if (expression.Field == null)
            {
                Visit(expression.Left, parameters);
                Visit(expression.Right, parameters);
                return;
            }

            Debug.Assert(expression.Field != null);

            switch (expression.Type)
            {
                case OperatorType.Equal:
                case OperatorType.LessThan:
                case OperatorType.GreaterThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThanEqual:
                    VisitFieldToken(QueryExpression.Extract(_queryText, expression.Field), expression.Value ?? expression.First, parameters);
                    return;
                case OperatorType.Between:
                    VisitFieldTokens(QueryExpression.Extract(_queryText, expression.Field), expression.First, expression.Second, parameters);
                    return;
                case OperatorType.In:
                    VisitFieldTokens(QueryExpression.Extract(_queryText, expression.Field), expression.Values, parameters);
                    return;
                case OperatorType.Method:
                    VisitMethodTokens(expression, parameters);
                    return;
                default:
                    throw new ArgumentException(expression.Type.ToString());
            }
        }

        protected ValueTokenType GetValueTokenType(BlittableJsonReaderObject parameters, ValueToken value, bool unwrapArrays)
        {
            var valueType = value.Type;

            if (valueType == ValueTokenType.Parameter)
            {
                var parameterName = QueryExpression.Extract(_queryText, value);

                if (parameters == null)
                    throw new InvalidOperationException();

                if (parameters.TryGetMember(parameterName, out var parameterValue) == false)
                    throw new InvalidOperationException();

                valueType = QueryBuilder.GetValueTokenType(parameterValue, unwrapArrays);
            }

            return valueType;
        }

        public abstract void VisitFieldToken(string fieldName, ValueToken value, BlittableJsonReaderObject parameters);

        public abstract void VisitFieldTokens(string fieldName, ValueToken firstValue, ValueToken secondValue, BlittableJsonReaderObject parameters);

        public abstract void VisitFieldTokens(string fieldName, List<ValueToken> values, BlittableJsonReaderObject parameters);

        public abstract void VisitMethodTokens(QueryExpression expression, BlittableJsonReaderObject parameters);
    }
}