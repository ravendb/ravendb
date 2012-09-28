using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;

namespace Jint.Expressions {
    public enum PropertyExpressionType {
        Data,
        Get,
        Set
    }

    [Serializable]
    public class PropertyDeclarationExpression : Expression {
        public PropertyDeclarationExpression() {

        }

        public string Name { get; set; }

        public Expression Expression { get; set; }

        public PropertyExpressionType Mode { get; set; }

        public Expression GetExpression { get; set; }

        public Expression SetExpression { get; set; }

        [DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

        internal void SetSet(PropertyDeclarationExpression propertyExpression) {
            SetExpression = propertyExpression.Expression;
        }

        internal void SetGet(PropertyDeclarationExpression propertyExpression) {
            GetExpression = propertyExpression.Expression;
        }
    }
}
