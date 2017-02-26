using System;
using System.Linq;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace Lambda2Js
{
    public static class LambdaExpressionExtensions
    {
        /// <summary>
        /// Compiles a lambda expression to JavaScript code.
        /// </summary>
        /// <param name="expr">Expression to compile to JavaScript.</param>
        /// <param name="options">
        /// Conversion options:
        /// whether to include only the body of the lambda,
        /// whether to use a single scope parameter,
        /// what extensions to use (i.e. StaticStringMethods, StaticMathMethods, or any other custom extensions).
        /// </param>
        /// <returns>JavaScript code represented as a string.</returns>
        public static string CompileToJavascript([NotNull] this LambdaExpression expr, JavascriptCompilationOptions options = null)
        {
            if (expr == null)
                throw new ArgumentNullException(nameof(expr));

            options = options ?? JavascriptCompilationOptions.DefaultOptions;

            if (options.ScopeParameter && expr.Parameters.Count != 1)
                throw new InvalidOperationException("When using ScopeParameter flag, the lambda expression must have one single argument.");

            var visitor =
                new JavascriptCompilerExpressionVisitor(
                    options.ScopeParameter ? expr.Parameters.Single() : null,
                    options.Extensions,
                    options);

            visitor.Visit(options.BodyOnly || options.ScopeParameter ? expr.Body : expr);

            if (options.BodyOnly || !options.ScopeParameter || visitor.UsedScopeMembers == null)
                return visitor.Result;

            return $"function({string.Join(",", visitor.UsedScopeMembers)}){{return {visitor.Result};}}";
        }

        /// <summary>
        /// Compiles an expression to JavaScript code.
        /// </summary>
        /// <param name="expr">Expression to compile to JavaScript.</param>
        /// <param name="options">
        /// Conversion options:
        /// What extensions to use (i.e. StaticStringMethods, StaticMathMethods, or any other custom extensions).
        /// Options not supported by this overload of `CompileToJavascript`:
        /// Include only the body of the lambda (supported only in the LambdaExpression overload of this method);
        /// Use a single scope parameter (supported only in the LambdaExpression overload of this method).
        /// </param>
        /// <returns>JavaScript code represented as a string.</returns>
        public static string CompileToJavascript([NotNull] this Expression expr, JavascriptCompilationOptions options = null)
        {
            if (expr == null)
                throw new ArgumentNullException(nameof(expr));

            options = options ?? JavascriptCompilationOptions.DefaultOptions;

            var visitor =
                new JavascriptCompilerExpressionVisitor(
                    null,
                    options.Extensions,
                    options);

            visitor.Visit(expr);

            return visitor.Result;
        }
    }
}