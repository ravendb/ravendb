using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class WithStatement : Statement {
        public Statement Statement { get; set; }
        public Expression Expression { get; set; }

        public WithStatement(Expression expression, Statement statement) {
            Statement = statement;
            Expression = expression;
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

    }
}
