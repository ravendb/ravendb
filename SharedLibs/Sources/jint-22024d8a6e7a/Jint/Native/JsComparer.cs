using System;
using System.Collections.Generic;
using System.Text;
using Jint.Expressions;

namespace Jint.Native {
    [Serializable]
    public class JsComparer : IComparer<JsInstance> {
        public IJintVisitor Visitor { get; set; }
        public JsFunction Function { get; set; }

        public JsComparer(IJintVisitor visitor, JsFunction function) {
            Visitor = visitor;
            Function = function;
        }

        public int Compare(JsInstance x, JsInstance y) {
            Visitor.Result = Function;
            new MethodCall(new List<Expression>() { new ValueExpression(x, TypeCode.Object), new ValueExpression(y, TypeCode.Object) }).Accept((IStatementVisitor)Visitor);
            return Math.Sign(Visitor.Result.ToNumber());
        }

    }
}
