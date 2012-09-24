using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class ExpressionStatement : Statement {
        public Expression Expression { get; set; }

        public ExpressionStatement(Expression expression) {
            Expression = expression;
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
