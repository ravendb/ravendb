using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class FunctionDeclarationStatement : Statement, IFunctionDeclaration {
        public string Name { get; set; }
        public List<string> Parameters { get; set; }
        public Statement Statement { get; set; }

        public FunctionDeclarationStatement() {
            Parameters = new List<string>();
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
