using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class SwitchStatement : Statement {
        public Statement Expression { get; set; }
        public List<CaseClause> CaseClauses { get; private set; }
        public Statement DefaultStatements { get; set; }

        public SwitchStatement() {
            CaseClauses = new List<CaseClause>();
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
