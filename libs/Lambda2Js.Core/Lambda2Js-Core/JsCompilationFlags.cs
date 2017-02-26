using System;
using System.Linq.Expressions;

namespace Lambda2Js
{
    [Flags]
    public enum JsCompilationFlags
    {
        /// <summary>
        /// Flag that indicates whether to compile only the body expression of the lambda.
        /// Applies only to <see cref="LambdaExpression"/>.
        /// <para>The lambda:</para>
        /// <para>() => x + y</para>
        /// <para>results in this kind of JavaScript:</para>
        /// <para>x+y</para>
        /// </summary>
        BodyOnly = 1,

        /// <summary>
        /// Flag that indicates whether the single argument of the lambda
        /// represents the arguments passed to the JavaScript.
        /// Applies only to <see cref="LambdaExpression"/>.
        /// <para>The lambda:</para>
        /// <para>(obj) => obj.X + obj.Y</para>
        /// <para>results in this kind of JavaScript:</para>
        /// <para>function(x,y){return x+y;}</para>
        /// </summary>
        ScopeParameter = 2,
    }
}