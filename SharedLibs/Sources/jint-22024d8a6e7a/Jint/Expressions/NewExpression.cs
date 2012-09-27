using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class NewExpression : Expression, IGenericExpression {
        // public List<Identifier> Identifiers { get; set; }
        public List<Expression> Arguments { get; set; }
        public Expression Expression { get; set; }
        
        public NewExpression(Expression expression) {
            Expression = expression;

            // Identifiers = new List<Identifier>();
            Arguments = new List<Expression>();
            Generics = new List<Expression>();
        }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

        public List<Expression> Generics { get; set; }
    }
}
