using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class ArrayDeclaration : Expression {
        public List<Statement> Parameters { get; set; }

        public ArrayDeclaration() {
            Parameters = new List<Statement>();
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

    }
}
