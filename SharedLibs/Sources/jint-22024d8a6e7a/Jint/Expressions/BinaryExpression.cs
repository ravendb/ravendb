using System;

namespace Jint.Expressions {
    [Serializable]
    public class BinaryExpression : Expression {
        public BinaryExpression(BinaryExpressionType type, Expression leftExpression, Expression rightExpression) {
            this.type = type;
            this.leftExpression = leftExpression;
            this.rightExpression = rightExpression;
        }

        private Expression leftExpression;

        public Expression LeftExpression {
            get { return leftExpression; }
            set { leftExpression = value; }
        }

        private Expression rightExpression;

        public Expression RightExpression {
            get { return rightExpression; }
            set { rightExpression = value; }
        }

        private BinaryExpressionType type;

        public BinaryExpressionType Type {
            get { return type; }
            set { type = value; }
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

        public override string ToString() {
            return Type.ToString() + " (" + leftExpression.ToString() + ", " + rightExpression.ToString() + ")";
        }
    }

    public enum BinaryExpressionType {
        And,
        Or,
        NotEqual,
        LesserOrEqual,
        GreaterOrEqual,
        Lesser,
        Greater,
        Equal,
        Minus,
        Plus,
        Modulo,
        Div,
        Times,
        Pow,
        BitwiseAnd,
        BitwiseOr,
        BitwiseXOr,
        Same,
        NotSame,
        LeftShift,
        RightShift,
        UnsignedRightShift,
        InstanceOf,
        In,
        Unknown
    }


}
