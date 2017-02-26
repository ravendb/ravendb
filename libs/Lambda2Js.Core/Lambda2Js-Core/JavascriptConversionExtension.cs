using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Lambda2Js
{
    /// <summary>
    /// Base class for extenions to the Lambda to JavaScript converter.
    /// </summary>
    public abstract class JavascriptConversionExtension
    {
        /// <summary>
        /// When overridden allows the control of expression nodes to JavaScript code.
        /// </summary>
        /// <param name="context">JavaScript conversion context, that allows to write text to the output stream.</param>
        public abstract void ConvertToJavascript(JavascriptConversionContext context);

        /// <summary>
        /// Helper method that gets the <see cref="System.Type"/> of an expression, ignoring type casts to base classes.
        /// </summary>
        /// <param name="expression">The expression to get the type for.</param>
        /// <returns></returns>
        protected static Type GetTypeOfExpression(Expression expression)
        {
            if (expression.NodeType == ExpressionType.Convert || expression.NodeType == ExpressionType.ConvertChecked)
                if (expression.Type.IsAssignableFrom(((UnaryExpression)expression).Operand.Type))
                    return GetTypeOfExpression(((UnaryExpression)expression).Operand);
            return expression.Type;
        }
    }
}