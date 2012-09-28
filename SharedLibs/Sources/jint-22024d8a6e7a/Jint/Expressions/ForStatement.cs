using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class ForStatement : Statement, IForStatement {
        public Statement InitialisationStatement { get; set; }
        public Statement ConditionExpression { get; set; }
        public Statement IncrementExpression { get; set; }
        public Statement Statement { get; set; }

        public ForStatement() {
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
