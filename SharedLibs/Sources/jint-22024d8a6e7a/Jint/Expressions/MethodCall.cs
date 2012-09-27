using System;
using System.Collections.Generic;

namespace Jint.Expressions {
    [Serializable]
    public class MethodCall : Expression, IGenericExpression {
        public MethodCall() {
            Arguments = new List<Expression>();
            Generics = new List<Expression>();
        }

        public MethodCall(List<Expression> arguments)
            : this() {
            Arguments.AddRange(arguments);
        }

        public List<Expression> Arguments { get; set; }
        public List<Expression> Generics { get; set; }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }
    }
}
