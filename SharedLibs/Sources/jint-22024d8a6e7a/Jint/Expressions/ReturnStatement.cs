using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class ReturnStatement : Statement {
        public Expression Expression { get; set; }

        public ReturnStatement() {
        }

        public ReturnStatement(Expression expression) {
            Expression = expression;
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

    }
}
