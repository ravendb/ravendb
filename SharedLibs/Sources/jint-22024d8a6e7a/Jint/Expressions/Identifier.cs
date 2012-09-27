using System;

namespace Jint.Expressions {
    [Serializable]
    public class Identifier : Expression, IAssignable {
        public Identifier(string text) {
            Text = text;
        }

        public string Text { get; set; }

        public override string ToString() {
            return Text;
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
