using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class ForEachInStatement : Statement, IForStatement {
        public Statement InitialisationStatement { get; set; }
        public Expression Expression { get; set; }
        public Statement Statement { get; set; }

        public ForEachInStatement() {
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
