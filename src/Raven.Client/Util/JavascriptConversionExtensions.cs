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
                var node = context.Node as MemberExpression;
                if (node != null && node.Member.Name == "Count" && IsCollection(node.Member.DeclaringType))
                {
                    var writer = context.GetWriter();
                    context.PreventDefault();

                    context.Visitor.Visit(node.Expression);

                    using (writer.Operation(node))
                    {
                        writer.Write(".");
                        writer.Write("length");
                    }

                    return;
                }

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
                    context.Visitor.Visit(obj.Expression);
                    javascriptWriter.Write($".{obj.Member.Name}.{newName}");
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

            private static bool IsCollection(Type type)
            {
                if (type.GetGenericArguments().Length == 0)
                    return false;

                return typeof(IEnumerable).IsAssignableFrom(type.GetGenericTypeDefinition());
            }
        }

        public class ReplaceParameterWithThis : JavascriptConversionExtension
        {
            public ParameterExpression Parameter;

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var parameter = context.Node as ParameterExpression;
                if (parameter == null || parameter != Parameter)
                    return;

                context.PreventDefault();
                var writer = context.GetWriter();

                using (writer.Operation(parameter))
                {
                    writer.Write("this");
                }
            }
        }

        public class DateTimeSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
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

                if (node.Type == typeof(DateTime))
                {
                    var writer = context.GetWriter();
                    context.PreventDefault();

                    using (writer.Operation(node))
                    {
                        //match DateTime expressions like user.DateOfBirth, order.ShipmentInfo.DeliveryDate, etc
                        if (node.Expression != null)
                        {
                            writer.Write("Date.parse(");
                            context.Visitor.Visit(node.Expression); //visit inner expression (user ,order.ShipmentInfo, etc)
                            writer.Write($".{node.Member.Name})");
                            return;
                        }

                        //match DateTime.Now , DateTime.UtcNow, DateTime.Today
                        switch (node.Member.Name)
                        {
                            case "Now":
                                writer.Write("Date.now()");
                                break;
                            case "UtcNow":
                                writer.Write(
                                    @"(function (date) { return new Date(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours(), date.getUTCMinutes(), date.getUTCSeconds(), date.getUTCMilliseconds());})(new Date()).getTime()");
                                break;
                            case "Today":
                                writer.Write("new Date().setHours(0,0,0,0)");
                                break;
                        }
                    }

                    return;
                }

                if (node.Expression.Type == typeof(DateTime) && node.Expression is MemberExpression memberExpression)
                {
                    var writer = context.GetWriter();
                    context.PreventDefault();

                    using (writer.Operation(node))
                    {
                        //match expressions like : DateTime.Today.Year , DateTime.Now.Day , user.Birthday.Month , etc

                        writer.Write("new Date(");

                        if (memberExpression.Member.DeclaringType != typeof(DateTime))
                        {
                            writer.Write($"Date.parse({node.Expression})");
                        }

                        writer.Write(")");

                        switch (node.Member.Name)
                        {
                            case "Year":
                                writer.Write(memberExpression.Member.Name == "UtcNow" ? ".getUTCFullYear()" : ".getFullYear()");
                                break;
                            case "Month":
                                writer.Write(memberExpression.Member.Name == "UtcNow" ? ".getUTCMonth()+1" : ".getMonth()+1");
                                break;
                            case "Day":
                                writer.Write(memberExpression.Member.Name == "UtcNow" ? ".getUTCDate()" : ".getDate()");
                                break;
                            case "Hour":
                                writer.Write(memberExpression.Member.Name == "UtcNow" ? ".getUTCHours()" : ".getHours()");
                                break;
                            case "Minute":
                                writer.Write(memberExpression.Member.Name == "UtcNow" ? ".getUTCMinutes()" : ".getMinutes()");
                                break;
                            case "Second":
                                writer.Write(memberExpression.Member.Name == "UtcNow" ? ".getUTCSeconds()" : ".getSeconds()");
                                break;
                            case "Millisecond":
                                writer.Write(memberExpression.Member.Name == "UtcNow" ? ".getUTCMilliseconds()" : ".getMilliseconds()");
                                break;
                            case "Ticks":
                                writer.Write(".getTime()*10000");
                                break;
                        }
                    }                 
                }                
            }
        }

        public class BooleanSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is ConstantExpression nodeAsConst) ||
                    nodeAsConst.Type != typeof(bool))
                    return;

                context.PreventDefault();
                var writer = context.GetWriter();
                var val = (bool)nodeAsConst.Value ? "true" : "false";

                using (writer.Operation(nodeAsConst))
                {
                    writer.Write(val);
                }
            }
        }
    }
}
