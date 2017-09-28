namespace Raven.Server.Documents.Queries.AST
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

        public override string ToString()
        {
            return Left + " " + Operator + " " + Right;
        }

        public override string GetText()
        {
            return ToString();
        }
    }
}
