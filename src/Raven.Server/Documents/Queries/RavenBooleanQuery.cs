using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Lucene.Net.Search;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Extensions;
using Query = Lucene.Net.Search.Query;

namespace Raven.Server.Documents.Queries
{
    public class RavenBooleanQuery : BooleanQuery
    {
        private readonly OperatorType _operator;

        public bool IsBoosted => Boost.AlmostEquals(1.0f) == false;
        
        public RavenBooleanQuery(OperatorType @operator)
        {
            _operator = @operator;
        }

        public RavenBooleanQuery(Query left, Query right, OperatorType @operator, List<string> buildSteps)
        {
            _operator = @operator;

            switch (@operator)
            {
                case OperatorType.And:
                    And(left, right, buildSteps);
                    break;

                case OperatorType.Or:
                    Or(left, right, buildSteps);
                    break;

                default:
                    ThrowInvalidOperatorType(@operator);
                    break;
            }
        }

        public bool TryAnd(Query right, List<string> buildSteps)
        {
            var canMergeClauses = true;
            if (_operator is not OperatorType.And)
            {
                canMergeClauses = false;
                buildSteps?.Add($"Cannot perform merging `{right}` into `{ToString()}` since this {nameof(RavenBooleanQuery)} has operator `{_operator}.");
            }
            else if (right is RavenBooleanQuery rightRbq && rightRbq._operator != _operator)
            {
                canMergeClauses = false;
                buildSteps?.Add($"Cannot perform merging `{rightRbq}` into `{ToString()}` since this {nameof(rightRbq)} has operator `{rightRbq._operator} and this {nameof(RavenBooleanQuery)} has {_operator}.");
            }
            // If this RavenBooleanQuery or the incoming Rbq has a boost, we cannot merge it.
            // When the right query is not a RavenBooleanQuery, we can merge it since it won't be boosted by this parent. 
            else if (IsBoosted || right is RavenBooleanQuery {IsBoosted: true}) 
            {
                canMergeClauses = false;
                buildSteps?.Add($"Cannot perform merging `{right}` into `{ToString()}` since boost is non-default. Left: {Boost} Right: {right.Boost}");
            }

            if (canMergeClauses)
            {
                AddInternal(right, Occur.MUST, OperatorType.And, buildSteps);
                return true;
            }

            return false;
        }

        public void And(Query left, Query right, List<string> buildSteps)
        {
            if (_operator != OperatorType.And)
                ThrowInvalidOperator(OperatorType.And);

            AddInternal(left, Occur.MUST, OperatorType.And, buildSteps);
            AddInternal(right, Occur.MUST, OperatorType.And, buildSteps);
        }
        
        public bool TryOr(Query right, List<string> buildSteps)
        {
            var canMergeClauses = true;
            if (_operator is not OperatorType.Or)
            {
                canMergeClauses = false;
                buildSteps?.Add($"Cannot perform merging `{right}` into `{ToString()}` since this {nameof(RavenBooleanQuery)} has operator `{_operator}.");
            }
            else if (right is RavenBooleanQuery rightRbq && rightRbq._operator != _operator)
            {
                canMergeClauses = false;
                buildSteps?.Add($"Cannot perform merging `{rightRbq}` into `{ToString()}` since this {nameof(rightRbq)} has operator `{rightRbq._operator} and this {nameof(RavenBooleanQuery)} has {_operator}.");
            }
            else if (IsBoosted || right is RavenBooleanQuery {IsBoosted: true})
            {
                canMergeClauses = false;
                buildSteps?.Add($"Cannot perform merging `{right}` into `{ToString()}` since boost is non-default. Left: {Boost} Right: {right.Boost}");
            }
            
            if (canMergeClauses)
            {
                AddInternal(right, Occur.SHOULD, OperatorType.Or, buildSteps);
                return true;
            }

            return false;
        }

        public void Or(Query left, Query right, List<string> buildSteps)
        {
            if (_operator != OperatorType.Or)
                ThrowInvalidOperator(OperatorType.Or);

            AddInternal(left, Occur.SHOULD, OperatorType.Or, buildSteps);
            AddInternal(right, Occur.SHOULD, OperatorType.Or, buildSteps);
        }

        private void AddInternal(Query query, Occur occur, OperatorType @operator, List<string> buildSteps)
        {
            if (query is RavenBooleanQuery booleanQuery)
            {
                if (booleanQuery._operator == @operator && booleanQuery.IsBoosted == false && IsBoosted == false)
                {
                    foreach (var booleanClause in booleanQuery.Clauses)
                        Add(booleanClause);

                    return;
                }
                else
                {
                    buildSteps?.Add(
                        $"Cannot apply query optimization because operator is {@operator}, but we got {booleanQuery._operator} with boosting '{booleanQuery.IsBoosted}' ({booleanQuery.Boost} - {SingleToInt32Bits(booleanQuery.Boost)})");
                }
            }

            buildSteps?.Add($"Cannot apply query optimization because query ({query}) is of type {query.GetType()}.");

            Add(query, occur);

            static unsafe int SingleToInt32Bits(float value)
            {
                return *(int*)(&value);
            }
        }

        public static unsafe int SingleToInt32Bits(float value)
        {
            return *(int*)(&value);
        }

        private void ThrowInvalidOperator(OperatorType @operator)
        {
            throw new InvalidOperationException($"Cannot '{@operator}' query clause because current operator is {_operator}");
        }

        private static void ThrowInvalidOperatorType(OperatorType operatorType)
        {
            throw new ArgumentException($"{nameof(RavenBooleanQuery)} doesn't handle '{operatorType}' operator");
        }
    }
}
