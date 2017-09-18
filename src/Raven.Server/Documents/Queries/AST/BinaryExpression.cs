using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Queries.Parser
{
    public class BinaryExpression : QueryExpression
    {
        public QueryExpression Left;
        public OperatorType Operator;
        public QueryExpression Right;

        public BinaryExpression(QueryExpression left, QueryExpression right, OperatorType op)
        {
            Left = left;
            Right = right;
            Operator = op;
            Type = ExpressionType.Binary;
        }
    }
}
