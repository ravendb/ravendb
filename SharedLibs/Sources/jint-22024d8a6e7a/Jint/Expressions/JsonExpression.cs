using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    [Serializable]
    public class JsonExpression : Expression {
        public JsonExpression() {
            Values = new Dictionary<string, Expression>();
        }

        public Dictionary<string, Expression> Values { get; set; }

        [System.Diagnostics.DebuggerStepThrough]
        public override void Accept(IStatementVisitor visitor) {
            visitor.Visit(this);
        }

        internal void Push(PropertyDeclarationExpression propertyExpression) {
            if (propertyExpression.Name == null) {
                propertyExpression.Name = propertyExpression.Mode.ToString().ToLower();
                propertyExpression.Mode = PropertyExpressionType.Data;
            }
            if (Values.ContainsKey(propertyExpression.Name)) {
                PropertyDeclarationExpression exp = Values[propertyExpression.Name] as PropertyDeclarationExpression;
                if (exp == null)
                    throw new JintException("A property cannot be both an accessor and data");
                switch (propertyExpression.Mode) {
                    case PropertyExpressionType.Data:
                        if (propertyExpression.Mode == PropertyExpressionType.Data)
                            Values[propertyExpression.Name] = propertyExpression.Expression;
                        else
                            throw new JintException("A property cannot be both an accessor and data");
                        break;
                    case PropertyExpressionType.Get:
                        exp.SetGet(propertyExpression);
                        break;
                    case PropertyExpressionType.Set:
                        exp.SetSet(propertyExpression);
                        break;
                }
            }
            else {
                Values.Add(propertyExpression.Name, propertyExpression);
                switch (propertyExpression.Mode) {
                    case PropertyExpressionType.Data:
                        Values[propertyExpression.Name] = propertyExpression;
                        break;
                    case PropertyExpressionType.Get:
                        propertyExpression.SetGet(propertyExpression);
                        break;
                    case PropertyExpressionType.Set:
                        propertyExpression.SetSet(propertyExpression);
                        break;
                }
            }
        }
    }
}
