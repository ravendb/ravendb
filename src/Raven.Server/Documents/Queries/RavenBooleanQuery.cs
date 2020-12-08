using System;
using System.Collections.Generic;
using Lucene.Net.Search;
using Raven.Server.Documents.Queries.AST;
using Query = Lucene.Net.Search.Query;

namespace Raven.Server.Documents.Queries
{
    public class RavenBooleanQuery : BooleanQuery
    {
        private readonly OperatorType _operator;

        public bool AnyBoost => Math.Abs(Boost - 1.0f) >= float.Epsilon;

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
                    AddInternal(left, Occur.MUST, OperatorType.And, buildSteps);
                    TryAnd(right, buildSteps);
                    break;

                case OperatorType.Or:
                    AddInternal(left, Occur.SHOULD, OperatorType.Or, buildSteps);
                    TryOr(right, buildSteps);
                    break;

                default:
                    ThrowInvalidOperatorType(@operator);
                    break;
            }
        }

        public bool TryAnd(Query right, List<string> buildSteps)
        {
            if (_operator == OperatorType.And)
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
            if (_operator == OperatorType.Or)
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
                if (booleanQuery._operator == @operator && booleanQuery.AnyBoost == false)
                {
                    foreach (var booleanClause in booleanQuery.Clauses)
                        Add(booleanClause);

                    return;
                }
                else
                {
                    buildSteps?.Add($"Cannot apply query optimization because operator is {@operator}, but we got {booleanQuery._operator} with boosting '{booleanQuery.AnyBoost}' ({booleanQuery.Boost})");
                }
            }

            buildSteps?.Add($"Cannot apply query optimization because query ({query}) is of type {query.GetType()}.");

            Add(query, occur);
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
