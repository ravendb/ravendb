using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace Lambda2Js
{
    /// <summary>
    /// Writes Javascript code.
    /// </summary>
    public class JavascriptWriter
    {
        private readonly StringBuilder result = new StringBuilder();
        private readonly List<JavascriptOperationTypes> operandTypes = new List<JavascriptOperationTypes>();

        /// <summary>
        /// Gets the generated Javascript code so far.
        /// </summary>
        /// <returns>The generated code.</returns>
        public override string ToString()
        {
            return this.result.ToString();
        }

        /// <summary>
        /// Gets the length of the rendered Javascript code so far.
        /// </summary>
        public int Length
        {
            get { return this.result.Length; }
        }

        /// <summary>
        /// Uses the precedence operator only if needed. i.e. '(' operations ')'.
        /// <para> - Writes '(' immediately, and returns a disposable that writes the ending ')'.</para>
        /// <para> - Pushes the operation into the stack, and pops it when disposed.</para>
        /// </summary>
        /// <param name="op">The operation that is going to be rendered inside the using block.</param>
        /// <returns>Disposable that renders the ending ')' of the precedence operator.</returns>
        public PrecedenceController Operation(JavascriptOperationTypes op)
        {
            return new PrecedenceController(this.result, this.operandTypes, op);
        }

        /// <summary>
        /// Uses the precedence operator only if needed. i.e. '(' operations ')'.
        /// <para> - Writes '(' immediately, and returns a disposable that writes the ending ')'.</para>
        /// <para> - Pushes the operation into the stack, and pops it when disposed.</para>
        /// </summary>
        /// <param name="node">The node representing the operation that is going to be rendered inside the using block.</param>
        /// <returns>Disposable that renders the ending ')' of the precedence operator.</returns>
        public IDisposable Operation(Expression node)
        {
            var op = JsOperationHelper.GetJsOperator(node.NodeType, node.Type);
            if (op == JavascriptOperationTypes.NoOp)
                return null;

            return new PrecedenceController(this.result, this.operandTypes, op);
        }

        /// <summary>
        /// Writes a single character to the output.
        /// </summary>
        /// <param name="ch">Character to write to the output.</param>
        /// <returns>The <see cref="JavascriptWriter"/>, allowing a fluent style.</returns>
        public JavascriptWriter Write(char ch)
        {
            this.result.Append(ch);
            return this;
        }

        /// <summary>
        /// Writes a literal value to the output.
        /// </summary>
        /// <param name="value">Value to write.</param>
        /// <returns>The <see cref="JavascriptWriter"/>, allowing a fluent style.</returns>
        public JavascriptWriter WriteLiteral(object value)
        {
            if (value is string || value is char)
            {
                this.result.Append('"');
                this.WriteLiteralStringContent(value);
                this.result.Append('"');
            }
            else if (TypeHelpers.IsNumericType(value.GetType()))
            {
                if (TypeHelpers.IsIntegerType(value.GetType()))
                    this.result.Append(value);
                else if (value is float || value is double || value is decimal)
                    this.result.AppendFormat("{0:E}", value);
            }
            return this;
        }

        public void WriteLiteralStringContent(object value)
        {
            this.result.Append(
                value.ToString()
                    .Replace("\\", "\\\\")
                    .Replace("\r", "\\r")
                    .Replace("\n", "\\n")
                    .Replace("\t", "\\t")
                    .Replace("\0", "\\0")
                    .Replace("\"", "\\\""));
        }

        /// <summary>
        /// Writes the operator that corresponds to the given expression type.
        /// </summary>
        /// <param name="expressionType">Expression type representing the operator to render..</param>
        /// <returns>The <see cref="JavascriptWriter"/>, allowing a fluent style.</returns>
        public JavascriptWriter WriteOperator(ExpressionType expressionType, Type type)
        {
            JsOperationHelper.WriteOperator(this.result, expressionType, type);
            return this;
        }

        /// <summary>
        /// Writes a string to the output.
        /// </summary>
        /// <param name="str">String to write to the output.</param>
        /// <returns>The <see cref="JavascriptWriter"/>, allowing a fluent style.</returns>
        public JavascriptWriter Write(string str)
        {
            this.result.Append(str);
            return this;
        }

        /// <summary>
        /// Writes any object to the output.
        /// </summary>
        /// <param name="value">String to write to the output.</param>
        /// <returns>The <see cref="JavascriptWriter"/>, allowing a fluent style.</returns>
        public JavascriptWriter Write(object value)
        {
            this.result.Append(value);
            return this;
        }

        /// <summary>
        /// Writes a formatted string to the output, using the given values.
        /// </summary>
        /// <param name="format">The format string laying out the passed values.</param>
        /// <param name="values">Values that are used to replace the place-holders of the format string.</param>
        /// <returns>The <see cref="JavascriptWriter"/>, allowing a fluent style.</returns>
        [JetBrains.Annotations.StringFormatMethod("format")]
        public JavascriptWriter WriteFormat(string format, params object[] values)
        {
            this.result.AppendFormat(format, values);
            return this;
        }
    }
}