using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class CommaOperatorStatement : Expression {
        class StatementInfo {
            public int index { get; private set; }
            public Statement statement { get; private set; }

            public StatementInfo(int i, Statement s) {
                index = i;
                statement = s;
            }
        }

        public List<Statement> Statements { get; set; }

        public CommaOperatorStatement() {
            Statements = new List<Statement>();
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

    }
}
