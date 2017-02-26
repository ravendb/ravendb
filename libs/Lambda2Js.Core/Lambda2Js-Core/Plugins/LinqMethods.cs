using System;
using System.Linq;
using System.Linq.Expressions;

namespace Lambda2Js
{
    /// <summary>
    /// Extension that allows the conversion of some Linq methods.
    /// </summary>
    public class LinqMethods : JavascriptConversionExtension
    {
        public override void ConvertToJavascript(JavascriptConversionContext context)
        {
            var methodCall = context.Node as MethodCallExpression;
            if (methodCall != null)
                if (methodCall.Method.DeclaringType == typeof(Enumerable))
                {
                    switch (methodCall.Method.Name)
                    {
                        case "Select":
                            {
                                context.PreventDefault();
                                var writer = context.GetWriter();
                                using (writer.Operation(JavascriptOperationTypes.Call))
                                {
                                    using (writer.Operation(JavascriptOperationTypes.IndexerProperty))
                                    {
                                        // public static IEnumerable<TResult> Select<TSource, TResult>(this IEnumerable<TSource> source, Func<TSource, TResult> selector)
                                        // public static IEnumerable<TResult> Select<TSource, TResult>(this IEnumerable<TSource> source, Func<TSource, int, TResult> selector)
                                        var pars = methodCall.Method.GetParameters();
                                        if (pars.Length != 2)
                                            throw new NotSupportedException("The `Enumerable.Select` method must have 2 parameters.");

                                        context.Visitor.Visit(methodCall.Arguments[0]);
                                        writer.Write(".map");
                                    }

                                    writer.Write('(');

                                    // separator
                                    using (writer.Operation(0))
                                        context.Visitor.Visit(methodCall.Arguments[1]);

                                    writer.Write(')');
                                }

                                return;
                            }

                        case "Where":
                            {
                                context.PreventDefault();
                                var writer = context.GetWriter();
                                using (writer.Operation(JavascriptOperationTypes.Call))
                                {
                                    using (writer.Operation(JavascriptOperationTypes.IndexerProperty))
                                    {
                                        // public static IEnumerable<TSource> Where<TSource>(this IEnumerable<TSource> source, Func<TSource, bool> predicate)
                                        // public static IEnumerable<TSource> Where<TSource>(this IEnumerable<TSource> source, Func<TSource, int, bool> predicate)
                                        var pars = methodCall.Method.GetParameters();
                                        if (pars.Length != 2)
                                            throw new NotSupportedException("The `Enumerable.Where` method must have 2 parameters.");

                                        context.Visitor.Visit(methodCall.Arguments[0]);
                                        writer.Write(".filter");
                                    }

                                    writer.Write('(');

                                    // separator
                                    using (writer.Operation(0))
                                        context.Visitor.Visit(methodCall.Arguments[1]);

                                    writer.Write(')');
                                }

                                return;
                            }

                        case "ToArray":
                            {
                                // Ecma Script 6+: use spread operator
                                // Other: use array `slice`
                                if (context.Options.ScriptVersion.Supports(JavascriptSyntax.ArraySpread))
                                {
                                    context.PreventDefault();
                                    var writer = context.GetWriter();
                                    using (writer.Operation(0))
                                    {
                                        writer.Write('[');
                                        writer.Write("...");
                                        using (writer.Operation(JavascriptOperationTypes.ParamIsolatedLhs))
                                            context.Visitor.Visit(methodCall.Arguments[0]);
                                        writer.Write(']');
                                    }
                                }
                                else
                                {
                                    context.PreventDefault();
                                    var writer = context.GetWriter();
                                    using (writer.Operation(JavascriptOperationTypes.Call))
                                    {
                                        using (writer.Operation(JavascriptOperationTypes.IndexerProperty))
                                        {
                                            context.Visitor.Visit(methodCall.Arguments[0]);
                                            writer.Write(".slice");
                                        }

                                        writer.Write('(');
                                        writer.Write(')');
                                    }
                                }

                                return;
                            }
                    }
                }
        }
    }
}
