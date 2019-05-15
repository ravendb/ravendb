using System;
using Lucene.Net.Search;
using Raven.Server.Documents.Queries.AST;
using Query = Lucene.Net.Search.Query;

namespace Raven.Server.Documents.Queries
{
    public class RavenBooleanQuery : BooleanQuery
    {
        private readonly OperatorType _operator;

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
                    Add(new BooleanClause(left, Occur.MUST));
                    TryAnd(right);
                    break;
                case OperatorType.Or:
                    Add(new BooleanClause(left, Occur.SHOULD));
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
                Add(right, Occur.MUST);
                return true;
            }

            return false;
        }

        public void And(Query left, Query right)
        {
            if (_operator != OperatorType.And)
                ThrowInvalidOperator(OperatorType.And);

            Add(left, Occur.MUST);
            Add(right, Occur.MUST);
        }

        public bool TryOr(Query right)
        {
            if (_operator == OperatorType.Or)
            {
                Add(right, Occur.SHOULD);
                return true;
            }

            return false;
        }

        public void Or(Query left, Query right)
        {
            if (_operator != OperatorType.Or)
                ThrowInvalidOperator(OperatorType.Or);

            Add(left, Occur.SHOULD);
            Add(right, Occur.SHOULD);
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
