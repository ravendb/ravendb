using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class VariableDeclarationStatement : Statement {
        public bool Global { get; set; }
        public string Identifier { get; set; }
        public Expression Expression { get; set; }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

    }
}
