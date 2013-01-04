using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class FunctionExpression : Expression, IFunctionDeclaration {
        public List<string> Parameters { get; set; }
        public Statement Statement { get; set; }
        public string Name { get; set; }

        public FunctionExpression() {
            Parameters = new List<string>();
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
