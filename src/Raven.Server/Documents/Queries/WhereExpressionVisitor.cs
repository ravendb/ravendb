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

        public void Visit(QueryExpression expression, BlittableJsonReaderObject parameters, bool isNegated)
        {
            if (expression is TrueExpression)
                return;

            if (expression is FieldExpression f)
            {
                VisitFieldToken(f, null, parameters, null, isNegated);
                return;
            }

            if (expression is BetweenExpression between)
            {
                VisitFieldToken(between.Source, null, parameters,OperatorType.GreaterThanEqual, isNegated);
                VisitBetween(between.Source, between.Min, between.Max, parameters, isNegated);
                return;
            }

            if (expression is InExpression ie)
            {
                VisitFieldToken(ie.Source, null, parameters, OperatorType.Equal, isNegated);
                VisitIn(ie.Source, ie.Values, parameters, isNegated);
                return;
            }
            if (expression is MethodExpression me)
            {
                VisitMethodTokens(me.Name, me.Arguments, parameters, isNegated);
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
                
                case OperatorType.LessThan:
                case OperatorType.GreaterThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThanEqual:
                    VisitBooleanMethod(be.Left, be.Right, be.Operator, parameters, isNegated);
                    return;
                case OperatorType.NotEqual:
                    VisitBooleanMethod(be.Left, be.Right, be.Operator, parameters, !isNegated);
                    return;
                case OperatorType.And:                
                case OperatorType.Or:                
                    Visit(be.Left, parameters, isNegated);
                    Visit(be.Right, parameters, isNegated);
                    break;
                case OperatorType.AndNot:
                case OperatorType.OrNot:
                    Visit(be.Left, parameters, isNegated);
                    Visit(be.Right, parameters, !isNegated);
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

        public abstract void VisitBooleanMethod(QueryExpression leftSide, QueryExpression rightSide, OperatorType operatorType, BlittableJsonReaderObject parameters, bool isNegated);
        
        public abstract void VisitFieldToken(QueryExpression fieldName, QueryExpression value, BlittableJsonReaderObject parameters, OperatorType? operatorType, bool isNegated);

        public abstract void VisitBetween(QueryExpression fieldName, QueryExpression firstValue, QueryExpression secondValue, BlittableJsonReaderObject parameters, bool isNegated);

        public abstract void VisitIn(QueryExpression fieldName, List<QueryExpression> values, BlittableJsonReaderObject parameters, bool isNegated);

        public abstract void VisitMethodTokens(StringSegment name, List<QueryExpression> arguments, BlittableJsonReaderObject parameters, bool isNegated);
    }
}
