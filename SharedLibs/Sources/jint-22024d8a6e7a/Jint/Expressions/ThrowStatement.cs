using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class ThrowStatement : Statement {
        public Expression Expression { get; set; }

        public ThrowStatement(Expression expression) {
            this.Expression = expression;
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

    }
}
