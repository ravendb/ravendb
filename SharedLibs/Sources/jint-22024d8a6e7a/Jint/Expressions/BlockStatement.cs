using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class BlockStatement: Statement {

        public LinkedList<Statement> Statements { get; set; }

        public BlockStatement() {
            Statements = new LinkedList<Statement>();
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
