using System;

namespace Jint.Expressions {
    [Serializable]
    public class ClrIdentifier : Expression {
        public ClrIdentifier(string text) {
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
