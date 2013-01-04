using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class TryStatement : Statement {
        public Statement Statement { get; set; }
        public FinallyClause Finally { get; set; }
        public CatchClause Catch { get; set; }

        public TryStatement() {
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
