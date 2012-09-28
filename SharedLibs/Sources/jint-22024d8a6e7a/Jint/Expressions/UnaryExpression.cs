using System;

namespace Jint.Expressions {
    [Serializable]
    public class UnaryExpression : Expression {
        public UnaryExpression(UnaryExpressionType type, Expression expression) {
            this.type = type;
            this.expression = expression;
        }

        private Expression expression;
        public Expression Expression {
            get { return expression; }
            set { expression = value; }
        }

        private UnaryExpressionType type;
        public UnaryExpressionType Type {
            get { return type; }
            set { type = value; }
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }

    public enum UnaryExpressionType {
        TypeOf,
        New,
        Not,
        Negate,
        Positive,
        PrefixPlusPlus,
        PrefixMinusMinus,
        PostfixPlusPlus,
        PostfixMinusMinus,
        Delete,
        Void,
        Inv,
        Unknown
    }
}
