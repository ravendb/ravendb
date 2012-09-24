using System;

namespace Jint.Expressions {
    [Serializable]
    public class PropertyExpression : Identifier, IAssignable {
        public PropertyExpression(string text)
            : base(text) {
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
