using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Lambda2Js;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;

namespace Raven.Client.Util
{
    internal class JavascriptConversionExtensions
    {
        public class CustomMethods : JavascriptConversionExtension
        {
            public readonly Dictionary<string, object> Parameters = new Dictionary<string, object>();
            public int Suffix { get; set; }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var methodCallExpression = context.Node as MethodCallExpression;

                var nameAttribute = methodCallExpression?
                    .Method
                    .GetCustomAttributes(typeof(JavascriptMethodNameAttribute), false)
                    .OfType<JavascriptMethodNameAttribute>()
                    .FirstOrDefault();

                if (nameAttribute == null)
                    return;
                context.PreventDefault();

                var javascriptWriter = context.GetWriter();
                javascriptWriter.Write(".");
                javascriptWriter.Write(nameAttribute.Name);
                javascriptWriter.Write("(");

                var args = new List<Expression>();
                foreach (var expr in methodCallExpression.Arguments)
                {
                    var expression = expr as NewArrayExpression;
                    if (expression != null)
                        args.AddRange(expression.Expressions);
                    else
                        args.Add(expr);
                }

                for (var i = 0; i < args.Count; i++)
                {
                    var name = $"arg_{Parameters.Count}_{Suffix}";
                    if (i != 0)
                        javascriptWriter.Write(", ");
                    javascriptWriter.Write("args.");
                    javascriptWriter.Write(name);
                    object val;
                    if (LinqPathProvider.GetValueFromExpressionWithoutConversion(args[i], out val))
                        Parameters[name] = val;
                }
                if (nameAttribute.PositionalArguments != null)
                {
                    for (int i = args.Count;
                        i < nameAttribute.PositionalArguments.Length;
                        i++)
                    {
                        if (i != 0)
                            javascriptWriter.Write(", ");
                        context.Visitor.Visit(Expression.Constant(nameAttribute.PositionalArguments[i]));
                    }
                }

                javascriptWriter.Write(")");
            }
        }

        public class LinqMethodsSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var methodCallExpression = context.Node as MethodCallExpression;
                var methodName = methodCallExpression?
                    .Method.Name;

                if (methodName == null)
                    return;

                string newName;
                switch (methodName)
                {
                    case "Any":
                        newName = "some";
                        break;
                    case "All":
                        newName = "every";
                        break;
                    case "Select":
                        newName = "map";
                        break;
                    case "Where":
                        newName = "filter";
                        break;
                    case "Contains":
                        newName = "indexOf";
                        break;
                    default:
                        return;

                }
                var javascriptWriter = context.GetWriter();

                var obj = methodCallExpression.Arguments[0] as MemberExpression;
                if (obj == null)
                {
                    if (methodCallExpression.Arguments[0] is MethodCallExpression innerMethodCall)
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(innerMethodCall);
                        javascriptWriter.Write($".{newName}");
                    }
                    else
                        return;
                }
                else
                {
                    context.PreventDefault();
                    javascriptWriter.Write($"this.{obj.Member.Name}.{newName}");
                }

                if (methodCallExpression.Arguments.Count < 2)
                    return;

                javascriptWriter.Write("(");
                context.Visitor.Visit(methodCallExpression.Arguments[1]);
                javascriptWriter.Write(")");

                if (newName == "indexOf")
                {
                    javascriptWriter.Write(">=0");
                }
            }
        }

        public class DatesAndConstantsSupport : JavascriptConversionExtension
        {
            public ParameterExpression Parameter;

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var nodeAsConst = context.Node as ConstantExpression;

                if (nodeAsConst != null && nodeAsConst.Type == typeof(bool))
                {
                    context.PreventDefault();
                    var writer = context.GetWriter();
                    var val = nodeAsConst.Value.ToString().ToLower();

                    using (writer.Operation(nodeAsConst))
                    {
                        writer.Write(val);
                    }

                    return;
                }

                var newExp = context.Node as NewExpression;

                if (newExp != null && newExp.Type == typeof(DateTime))
                {
                    context.PreventDefault();
                    var writer = context.GetWriter();
                    using (writer.Operation(newExp))
                    {
                        writer.Write("new Date(");

                        for (int i = 0; i < newExp.Arguments.Count; i++)
                        {
                            var value = ((ConstantExpression)newExp.Arguments[i]).Value;
                            if (i == 1)
                            {
                                var month = (int)value;
                                writer.Write(month - 1);
                            }
                            else
                            {
                                writer.Write(value);
                            }
                            if (i < newExp.Arguments.Count - 1)
                            {
                                writer.Write(", ");
                            }
                        }
                        writer.Write(")");
                    }

                    return;
                }

                var node = context.Node as MemberExpression;
                if (node == null)
                    return;

                context.PreventDefault();
                var javascriptWriter = context.GetWriter();

                using (javascriptWriter.Operation(node))
                {
                    if (node.Type == typeof(DateTime))
                    {
                        //match DateTime expressions like call.Started, user.DateOfBirth, etc
                        if (node.Expression == Parameter)
                        {
                            //translate it to Date.parse(this.Started)
                            javascriptWriter.Write($"Date.parse(this.{node.Member.Name})");
                            return;

                        }

                        //match expression where DateTime object is nested, like order.ShipmentInfo.DeliveryDate
                        if (node.Expression != null)
                        {

                            javascriptWriter.Write("Date.parse(");
                            context.Visitor.Visit(node.Expression); //visit inner expression (i.e order.ShipmentInfo)
                            javascriptWriter.Write($".{node.Member.Name})");
                            return;
                        }

                        //match DateTime.Now , DateTime.UtcNow, DateTime.Today
                        switch (node.Member.Name)
                        {
                            case "Now":
                                javascriptWriter.Write("Date.now()");
                                break;
                            case "UtcNow":
                                javascriptWriter.Write(@"(function (date) { return new Date(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours(), date.getUTCMinutes(), date.getUTCSeconds(), date.getUTCMilliseconds());})(new Date()).getTime()");
                                break;
                            case "Today":
                                javascriptWriter.Write("new Date().setHours(0,0,0,0)");
                                break;
                        }
                        return;
                    }

                    if (node.Expression == Parameter)
                    {
                        javascriptWriter.Write($"this.{node.Member.Name}");
                        return;
                    }

                    switch (node.Expression)
                    {
                        case null:
                            return;
                        case MemberExpression member:
                            context.Visitor.Visit(member);
                            break;
                        default:
                            context.Visitor.Visit(node.Expression);
                            break;
                    }

                    javascriptWriter.Write(".");

                    if (node.Member.Name == "Count" && IsCollection(node.Member.DeclaringType))
                    {
                        javascriptWriter.Write("length");
                    }
                    else
                    {
                        javascriptWriter.Write(node.Member.Name);
                    }
                }
            }

            private static bool IsCollection(Type type)
            {
                if (type.GetGenericArguments().Length == 0)
                    return false;

                return typeof(IEnumerable).IsAssignableFrom(type.GetGenericTypeDefinition());
            }
        }

    }
}
