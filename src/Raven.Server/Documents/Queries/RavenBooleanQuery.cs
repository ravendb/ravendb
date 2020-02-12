using System;
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

        public RavenBooleanQuery(Query left, Query right, OperatorType @operator)
        {
            _operator = @operator;

            switch (@operator)
            {
                case OperatorType.And:
                    AddInternal(left, Occur.MUST, OperatorType.And);
                    TryAnd(right);
                    break;
                case OperatorType.Or:
                    AddInternal(left, Occur.SHOULD, OperatorType.Or);
                    TryOr(right);
                    break;
                default:
                    ThrowInvalidOperatorType(@operator);
                    break;
            }
        }

        public bool TryAnd(Query right)
        {
            if (_operator == OperatorType.And)
            {
                AddInternal(right, Occur.MUST, OperatorType.And);
                return true;
            }

            return false;
        }

        public void And(Query left, Query right)
        {
            if (_operator != OperatorType.And)
                ThrowInvalidOperator(OperatorType.And);

            AddInternal(left, Occur.MUST, OperatorType.And);
            AddInternal(right, Occur.MUST, OperatorType.And);
        }

        public bool TryOr(Query right)
        {
            if (_operator == OperatorType.Or)
            {
                AddInternal(right, Occur.SHOULD, OperatorType.Or);
                return true;
            }

            return false;
        }

        public void Or(Query left, Query right)
        {
            if (_operator != OperatorType.Or)
                ThrowInvalidOperator(OperatorType.Or);

            AddInternal(left, Occur.SHOULD, OperatorType.Or);
            AddInternal(right, Occur.SHOULD, OperatorType.Or);
        }

        private void AddInternal(Query query, Occur occur, OperatorType @operator)
        {
            if (query is RavenBooleanQuery booleanQuery)
            {
                if (booleanQuery._operator == @operator && booleanQuery.AnyBoost == false)
                {
                    foreach (var booleanClause in booleanQuery.Clauses)
                        Add(booleanClause);

                    return;
                }
            }

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
