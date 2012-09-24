using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    /// <summary>
    /// A MemberExpression represents an elements which applies on a previous Expression
    /// </summary>
    [Serializable]
    public class MemberExpression : Expression {
        public Expression Member { get; set; }
        public Expression Previous { get; set; }

        public MemberExpression(Expression member, Expression previous) {
            Member = member;
            Previous = previous;
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

    }
}
