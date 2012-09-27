using System;

namespace Jint.Expressions {
    [Serializable]
    public class Indexer : Expression, IAssignable {
        public Indexer() {
        }

        public Indexer(Expression index) {
            this.Index = index;
        }

        public Expression Index { get; set; }

        public override string ToString() {
            return "[" + Index.ToString() + "]" + base.ToString();
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
