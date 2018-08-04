using System;

namespace Raven.Server.Documents.Queries.AST
{
    public class BinaryExpression : QueryExpression
    {
        public QueryExpression Left;
        public OperatorType Operator;
        public QueryExpression Right;
        public bool Parenthesis;

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

        public override string GetText(IndexQueryServerSide parent)
        {
            string op;
            switch (Operator)
            {
                case OperatorType.Equal:
                    op = "=";
                    break;
                case OperatorType.NotEqual:
                    op = "!=";
                    break;
                case OperatorType.LessThan:
                    op = "<";
                    break;
                case OperatorType.GreaterThan:
                    op = ">";
                    break;
                case OperatorType.LessThanEqual:
                    op = "<=";
                    break;
                case OperatorType.GreaterThanEqual:
                    op = ">=";
                    break;
                case OperatorType.And:
                    op = "and";
                    break;
                case OperatorType.Or:
                    op = "or";
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return Left.GetText(parent) + " " + op + " " + Right.GetText(parent);
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is BinaryExpression be))
                return false;

            return Operator == be.Operator && Left.Equals(be.Left) && Right.Equals(be.Right);
        }
    }
}
