using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class AssignmentExpression : Expression {
        public Expression Left { get; set; }
        public Expression Right { get; set; }
        public AssignmentOperator AssignmentOperator { get; set; }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }

    public enum AssignmentOperator {
        Assign,
        Multiply,
        Divide,
        Modulo,
        Add,
        Substract,
        ShiftLeft,
        ShiftRight,
        UnsignedRightShift,
        And,
        Or,
        XOr,
    }
}
