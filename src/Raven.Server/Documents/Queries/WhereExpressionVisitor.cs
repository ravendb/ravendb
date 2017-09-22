using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public abstract class WhereExpressionVisitor
    {
        protected readonly string QueryText;

        protected WhereExpressionVisitor(string queryText)
        {
            QueryText = queryText;
        }

        public void Visit(QueryExpression expression, BlittableJsonReaderObject parameters)
        {
            if (expression is TrueExpression)
                return;

            if (expression is FieldExpression f)
            {
                VisitFieldToken(f, null, parameters);
                return;
            }

            if (expression is BetweenExpression between)
            {
                VisitFieldToken(between.Source, null, parameters);
                VisitBetween(between.Source, between.Min, between.Max, parameters);
                return;
            }

            if (expression is InExpression ie)
            {
                VisitFieldToken(ie.Source, null, parameters);
                VisitIn(ie.Source, ie.Values, parameters);
                return;
            }
            if (expression is MethodExpression me)
            {
                VisitMethodTokens(me.Name, me.Arguments, parameters);
                return;
            }

            if (!(expression is BinaryExpression be))
            {
                ThrowUnexpectedExpression(expression, parameters);
                return;// never hit
            }

            switch (be.Operator)
            {
                case OperatorType.Equal:
                case OperatorType.NotEqual:
                case OperatorType.LessThan:
                case OperatorType.GreaterThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThanEqual:
                    VisitFieldToken(be.Left, be.Right, parameters);
                    return;
                case OperatorType.And:
                case OperatorType.AndNot:
                case OperatorType.Or:
                case OperatorType.OrNot:
                    Visit(be.Left, parameters);
                    Visit(be.Right, parameters);
                    break;

                default:
                    ThrowInvalidOperatorType(expression);
                    break;
            }
        }

        private static void ThrowInvalidOperatorType(QueryExpression expression)
        {
            throw new ArgumentException(expression.Type.ToString());
        }

        private void ThrowUnexpectedExpression(QueryExpression expression, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Expected binary expression, but got " + expression, QueryText, parameters);
        }

        protected ValueTokenType GetValueTokenType(BlittableJsonReaderObject parameters, ValueExpression value, bool unwrapArrays)
        {
            if (value.Value == ValueTokenType.Parameter)
            {
                if (parameters == null)
                {
                    QueryBuilder.ThrowParametersWereNotProvided(QueryText);
                    return ValueTokenType.Null; // never hit
                }

                if (parameters.TryGetMember(value.Token, out var parameterValue) == false)
                    QueryBuilder.ThrowParameterValueWasNotProvided(value.Token, QueryText, parameters);

                return QueryBuilder.GetValueTokenType(parameterValue, QueryText, parameters, unwrapArrays);
            }

            return value.Value;
        }

        public abstract void VisitFieldToken(QueryExpression fieldName, QueryExpression value, BlittableJsonReaderObject parameters);

        public abstract void VisitBetween(QueryExpression fieldName, QueryExpression firstValue, QueryExpression secondValue, BlittableJsonReaderObject parameters);

        public abstract void VisitIn(QueryExpression fieldName, List<QueryExpression> values, BlittableJsonReaderObject parameters);

        public abstract void VisitMethodTokens(StringSegment name, List<QueryExpression> arguments, BlittableJsonReaderObject parameters);
    }
}
