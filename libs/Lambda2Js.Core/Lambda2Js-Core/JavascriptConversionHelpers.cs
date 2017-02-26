using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using JetBrains.Annotations;

namespace Lambda2Js
{
    /// <summary>
    /// Extension methods to help with the JavaScript rendering, when implementing <see cref="JavascriptConversionExtension"/>.
    /// </summary>
    public static class JavascriptConversionHelpers
    {
        /// <summary>
        /// Writes many expression nodes, separated by the given separator.
        /// </summary>
        /// <param name="context">The Javascript conversion context.</param>
        /// <param name="node">The node to write.</param>
        /// <returns>The Javascript conversion context itself, to allow fluent style rendering.</returns>
        public static JavascriptConversionContext Write(this JavascriptConversionContext context, Expression node)
        {
            context.Visitor.Visit(node);
            return context;
        }

        /// <summary>
        /// Writes a single character.
        /// </summary>
        /// <param name="context">The Javascript conversion context.</param>
        /// <param name="ch">Character to write.</param>
        /// <returns>The Javascript conversion context itself, to allow fluent style rendering.</returns>
        public static JavascriptConversionContext Write(this JavascriptConversionContext context, char ch)
        {
            var writer = context.GetWriter();
            writer.Write(ch);
            return context;
        }

        /// <summary>
        /// Writes many expression nodes, separated by the given separator.
        /// </summary>
        /// <param name="context">The Javascript conversion context.</param>
        /// <param name="separator">Separator to be used.</param>
        /// <param name="nodes">A list of nodes to write.</param>
        /// <returns>The Javascript conversion context itself, to allow fluent style rendering.</returns>
        public static JavascriptConversionContext WriteMany(
            this JavascriptConversionContext context,
            char separator,
            IEnumerable<Expression> nodes)
        {
            var writer = context.GetWriter();
            int count = 0;
            foreach (var node in nodes)
            {
                if (count++ > 0)
                    writer.Write(separator);

                context.Visitor.Visit(node);
            }
            return context;
        }

        /// <summary>
        /// Writes many expression nodes, separated by the given separator.
        /// </summary>
        /// <param name="context">The Javascript conversion context.</param>
        /// <param name="separator">Separator to be used.</param>
        /// <param name="nodes">A list of nodes to write.</param>
        /// <returns>The Javascript conversion context itself, to allow fluent style rendering.</returns>
        public static JavascriptConversionContext WriteMany(
            this JavascriptConversionContext context,
            char separator,
            params Expression[] nodes)
        {
            return WriteMany(context, separator, (IEnumerable<Expression>)nodes);
        }

        /// <summary>
        /// Writes many expressions isolated from outer and inner operations by opening, closing and separator characters.
        /// </summary>
        /// <param name="context">The Javascript conversion context.</param>
        /// <param name="opening">First character to render, isolating from outer operation.</param>
        /// <param name="closing">Last character to render, isolating from outer operation.</param>
        /// <param name="separator">Separator character to render, isolating one parameter from the other.</param>
        /// <param name="nodes">Nodes to render.</param>
        public static JavascriptConversionContext WriteManyIsolated(
            this JavascriptConversionContext context,
            char opening,
            char closing,
            char separator,
            IEnumerable<Expression> nodes)
        {
            var writer = context.GetWriter();
            writer.Write(opening);
            using (writer.Operation(0))
                context.WriteMany(separator, nodes);
            writer.Write(closing);
            return context;
        }

        /// <summary>
        /// Writes many expressions isolated from outer and inner operations by opening, closing and separator characters.
        /// </summary>
        /// <param name="context">The Javascript conversion context.</param>
        /// <param name="opening">First character to render, isolating from outer operation.</param>
        /// <param name="closing">Last character to render, isolating from outer operation.</param>
        /// <param name="separator">Separator character to render, isolating one parameter from the other.</param>
        /// <param name="nodes">Nodes to render.</param>
        public static JavascriptConversionContext WriteManyIsolated(
            this JavascriptConversionContext context,
            char opening,
            char closing,
            char separator,
            params Expression[] nodes)
        {
            return WriteManyIsolated(context, opening, closing, separator, (IEnumerable<Expression>)nodes);
        }

        /// <summary>
        /// Encloses the following write calls in an operation context,
        /// that will automatically write precedence operators '(' and ')' if needed,
        /// depening on the operations stack.
        /// <para>
        /// For example, a sum (+)
        /// inside a multiplication (*) requires the precedence operators.
        /// </para>
        /// <para>
        /// To isolate operations, you can pass 0 to the <paramref name="op"/> parameter,
        /// making neither the current nor the inner operation need precedence operators.
        /// </para>
        /// </summary>
        /// <param name="context">Context of the conversion.</param>
        /// <param name="op">The operation that you want to render in the scope.</param>
        /// <returns>A disposable object that renders the ending ')' when needed.</returns>
        public static IDisposable Operation(
            this JavascriptConversionContext context,
            JavascriptOperationTypes op)
        {
            return context.GetWriter().Operation(op);
        }

        /// <summary>
        /// Writes a string to the output.
        /// </summary>
        /// <param name="context">The Javascript conversion context.</param>
        /// <param name="str">String to write.</param>
        /// <returns>The Javascript conversion context itself, to allow fluent style rendering.</returns>
        public static JavascriptConversionContext Write(this JavascriptConversionContext context, string str)
        {
            context.GetWriter().Write(str);
            return context;
        }

        /// <summary>
        /// Writes a formatted string to the output.
        /// </summary>
        /// <param name="context">The Javascript conversion context.</param>
        /// <param name="format">A string with place-holders to be filled with the values.</param>
        /// <param name="values">A list values to use in each place-holder.</param>
        /// <returns>The Javascript conversion context itself, to allow fluent style rendering.</returns>
        [StringFormatMethod("format")]
        public static JavascriptConversionContext WriteFormat(this JavascriptConversionContext context, string format, params object[] values)
        {
            context.GetWriter().WriteFormat(format, values);
            return context;
        }

        /// <summary>
        /// Writes a property accessor.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="propertyName">The property to access. Anything that can be converted to a string.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static JavascriptConversionContext WriteAccessor(
            [NotNull] this JavascriptConversionContext context,
            [NotNull] string propertyName)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));
            if (propertyName == null)
                throw new ArgumentNullException(nameof(propertyName));

            if (Regex.IsMatch(propertyName, @"^\w[\d\w]*$"))
            {
                context.GetWriter().Write('.');
                context.GetWriter().Write(propertyName);
            }
            else if (Regex.IsMatch(propertyName, @"^\d+$"))
            {
                context.GetWriter().Write('[');
                context.GetWriter().Write(propertyName);
                context.GetWriter().Write(']');
            }
            else
            {
                context.GetWriter().Write('[');
                context.GetWriter().WriteLiteral(propertyName);
                context.GetWriter().Write(']');
            }

            return context;
        }
    }
}