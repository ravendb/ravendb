using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class WhileStatement : Statement {
        public Expression Condition { get; set; }
        public Statement Statement { get; set; }

        public WhileStatement(Expression condition, Statement statement) {
            Condition = condition;
            Statement = statement;
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
