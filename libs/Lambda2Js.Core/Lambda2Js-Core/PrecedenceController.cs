using System;
using System.Collections.Generic;
using System.Text;

namespace Lambda2Js
{
    public class PrecedenceController : IDisposable
    {
        private readonly StringBuilder result;
        private readonly List<JavascriptOperationTypes> operandTypes;
        private readonly JavascriptOperationTypes op;

        public PrecedenceController(StringBuilder result, List<JavascriptOperationTypes> operandTypes, JavascriptOperationTypes op)
        {
            this.result = result;
            this.operandTypes = operandTypes;
            this.op = op;
            operandTypes.Add(op);
            this.WritePrecedenceCharIfNeeded('(');
        }

        public void Dispose()
        {
            this.WritePrecedenceCharIfNeeded(')');
            this.operandTypes.RemoveAt(this.operandTypes.Count - 1);
        }

        private void WritePrecedenceCharIfNeeded(char ch)
        {
            if (this.op != 0 && !this.CurrentHasPrecedence())
            {
                // The current operator does not have precedence
                // over it's parent operator. We need to
                // force the current operation precedence,
                // using the given precedence opertaor.
                this.result.Append(ch);
            }
        }

        private bool CurrentHasPrecedence()
        {
            var cnt = this.operandTypes.Count;

            if (cnt < 2)
                return true;

            var current = this.operandTypes[cnt - 1];
            var parent = this.operandTypes[cnt - 2];

            return JsOperationHelper.CurrentHasPrecedence(current, parent);
        }
    }
}