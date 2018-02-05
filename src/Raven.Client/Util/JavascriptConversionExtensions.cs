using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Lambda2Js;
using Newtonsoft.Json;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;

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
                if (context.Node is MemberExpression node && node.Member.Name == "Count" && IsCollection(node.Member.DeclaringType))
                {
                    HandleCount(context, node.Expression);
                    return;
                }

                var methodCallExpression = context.Node as MethodCallExpression;
                var methodName = methodCallExpression?
                    .Method.Name;

                if (methodName == null || IsCollection(methodCallExpression.Method.DeclaringType) == false)
                    return;

                string newName;
                switch (methodName)
                {
                    case "Any":
                    {
                        if (methodCallExpression.Arguments.Count > 1)
                        {
                            newName = "some";
                            break;
                        }

                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            {
                                writer.Write(".length > 0");
                                return;
                            }
                        }
                    }
                    case "All":
                        newName = "every";
                        break;
                    case "Select":
                    case "Sum":
                        newName = "map";
                        break;
                    case "Where":
                        newName = "filter";
                        break;
                    case "IndexOf":
                    case "Contains":
                        newName = "indexOf";
                        break;
                    case "ToList":
                    case "ToArray":
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        return;

                    case "Concat":
                        newName = "concat";
                        break;
                    case "ToDictionary":
                    {
                        context.PreventDefault();
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            context.Visitor.Visit(methodCallExpression.Arguments[0]);
                            writer.Write(".reduce(function(_obj, _cur) {");
                            writer.Write("_obj[");
                            context.Visitor.Visit(methodCallExpression.Arguments[1]);
                            writer.Write("(_cur)] = ");

                            if (methodCallExpression.Arguments.Count > 2)
                            {
                                context.Visitor.Visit(methodCallExpression.Arguments[2]);
                                writer.Write("(_cur);");
                            }
                            else
                            {
                                writer.Write("_cur;");
                            }

                            writer.Write("return _obj;");
                            writer.Write("}, {})");
                        }
                        return;
                    }
                    case "FirstOrDefault":
                    case "First":
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            if (methodCallExpression.Arguments.Count > 1)
                            {
                                writer.Write(".find");
                                context.Visitor.Visit(methodCallExpression.Arguments[1]);
                                return;
                            }
                            
                            writer.Write("[0]");
                            return;
                        }
                    }
                    case "Last":
                    case "LastOrDefault":
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            if (methodCallExpression.Arguments.Count > 1)
                            {
                                writer.Write(".slice().reverse().find");
                                context.Visitor.Visit(methodCallExpression.Arguments[1]);
                                return;
                            }
                            // arrayExpr.slice([-1])[0] will get the last value
                            writer.Write(".slice(-1)[0]");
                            return;

                        }
                    }
                    case "ElementAt":
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            writer.Write("[");
                            context.Visitor.Visit(methodCallExpression.Arguments[1]);
                            writer.Write("]");
                            return;

                        }
                    }
                    case "Reverse":
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            writer.Write(".slice().reverse()");
                            return;
                        }
                    }
                    case "Max":
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            if (methodCallExpression.Arguments.Count > 1)
                            {
                                writer.Write(".map");
                                context.Visitor.Visit(methodCallExpression.Arguments[1]);
                            }
                            writer.Write(".reduce(function(a, b) { return Math.max(a, b);})");
                            return;

                        }
                    }
                    case "Min":
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            if (methodCallExpression.Arguments.Count > 1)
                            {
                                writer.Write(".map");
                                context.Visitor.Visit(methodCallExpression.Arguments[1]);
                            }
                            writer.Write(".reduce(function(a, b) { return Math.min(a, b);})");
                            return;

                        }
                    }
                    case "Skip":
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            writer.Write(".slice(");
                            context.Visitor.Visit(methodCallExpression.Arguments[1]);
                            writer.Write(", ");
                            context.Visitor.Visit(methodCallExpression.Arguments[0]);
                            writer.Write(".length)");
                            return;
                        }
                    }
                    case "Take":
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            writer.Write(".slice(0, ");
                            context.Visitor.Visit(methodCallExpression.Arguments[1]);
                            writer.Write(")");
                            return;
                        }
                    }
                    case "Distinct":
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            writer.Write(".filter((value, index) => ");
                            context.Visitor.Visit(methodCallExpression.Arguments[0]);
                            writer.Write(".indexOf(value) == index)");
                            return;
                        }
                    }
                    case "DefaultIfEmpty":
                    {
                        context.PreventDefault();

                        object defaultVal = null;                       
                        var genericArguments = methodCallExpression.Arguments[0].Type.GetGenericArguments();
                        if (genericArguments.Length > 0)
                        {
                            if (genericArguments[0] == typeof(bool))
                            {
                                defaultVal = "false";
                            }
                            else if (genericArguments[0] == typeof(char))
                            {
                                defaultVal = '0';
                            }
                            else
                            {
                                defaultVal = GetDefault(genericArguments[0]);
                            }                            
                        }
                        
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            writer.Write($"(function(arr){{return arr.length > 0 ? arr : [{defaultVal?? "null"}]}})(");
                            context.Visitor.Visit(methodCallExpression.Arguments[0]);
                            writer.Write($")");
                            return;
                        }
                    }
                    case "SelectMany":
                    {
                        if (methodCallExpression.Arguments.Count > 2)
                            return;
                        
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {                                                                                                                
                            if (IsDictionary(methodCallExpression.Arguments[0].Type))
                            {
                                if (!(methodCallExpression.Arguments[1] is LambdaExpression lambda) ||
                                    !(lambda.Body is MemberExpression member) ||
                                    member.Member.Name != "Key" && member.Member.Name != "Value")
                                    return;
                                
                                context.PreventDefault();

                                writer.Write("Object.getOwnPropertyNames(");
                                context.Visitor.Visit(methodCallExpression.Arguments[0]);
                                writer.Write(")");

                                if (member.Member.Name == "Value")
                                {
                                    writer.Write(".map(function(k){return ");
                                    context.Visitor.Visit(methodCallExpression.Arguments[0]);
                                    writer.Write("[k]})");
                                }
                                
                                writer.Write(".reduce(function(a, b) { return a.concat(b);},[])");

                            }
                            
                            else
                            {
                                context.PreventDefault();
                                
                                context.Visitor.Visit(methodCallExpression.Arguments[0]);
                                writer.Write(".reduce(function(a, b) { return a.concat(");
                                context.Visitor.Visit(methodCallExpression.Arguments[1]);
                                writer.Write("(b)); }, [])");
                            }
                                                        
                            return;
                        }
                    }
                    case "Count":
                        HandleCount(context, methodCallExpression.Arguments[0]);
                        return;
                    default:
                        return;
                }

                var javascriptWriter = context.GetWriter();
                context.PreventDefault();

                if (methodCallExpression.Object != null)
                {
                    context.Visitor.Visit(methodCallExpression.Object);
                    javascriptWriter.Write($".{newName}");
                    javascriptWriter.Write("(");
                    context.Visitor.Visit(methodCallExpression.Arguments[0]);
                    javascriptWriter.Write(")");
                }
                else
                {
                    var typeInfo = methodCallExpression.Arguments[0].Type.GetTypeInfo();
                    if (typeInfo.IsGenericType &&
                           (
                            typeInfo.GetGenericTypeDefinition() == typeof(Dictionary<,>) ||
                             typeInfo.GetGenericTypeDefinition() == typeof(IDictionary<,>)
                           )
                        )
                    {
                        javascriptWriter.Write("Object.keys(");
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        javascriptWriter.Write(").reduce(function(state,index) { ");

                        javascriptWriter.Write(" state.push(");
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        javascriptWriter.Write("[index]); return state; }, [])");


                    }
                    else
                    {
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                    }

                    javascriptWriter.Write($".{newName}");
                    javascriptWriter.Write("(");
                    if (methodCallExpression.Arguments.Count > 1)
                    {
                        context.Visitor.Visit(methodCallExpression.Arguments[1]);
                    }
                    javascriptWriter.Write(")");
                }

                if (methodName == "Sum")
                {
                    javascriptWriter.Write(".reduce(function(a, b) { return a + b; }, 0)");
                }

                if (methodName == "Contains")
                {
                    javascriptWriter.Write(">=0");
                }
            }

            private static void HandleCount(JavascriptConversionContext context, Expression expression)
            {
                var writer = context.GetWriter();
                context.PreventDefault();

                context.Visitor.Visit(expression);

                using (writer.Operation(expression))
                {
                    writer.Write(".length");
                }
            }

            public static bool IsCollection(Type type)
            {
                if (type.GetGenericArguments().Length == 0)
                    return type == typeof(Enumerable);

                return typeof(IEnumerable).IsAssignableFrom(type.GetGenericTypeDefinition());
            }
            
            public static object GetDefault(Type type)
            {
                if (type.GetTypeInfo().IsValueType)
                {
                    return Activator.CreateInstance(type);
                }
                return null;
            }
            
            public static bool IsDictionary(Type type)
            {
                if (type.GetGenericArguments().Length == 0)
                    return type == typeof(Dictionary<,>) || type == typeof(IDictionary<,>);

                return typeof(Dictionary<,>).IsAssignableFrom(type.GetGenericTypeDefinition()) ||
                       typeof(IDictionary<,>).IsAssignableFrom(type.GetGenericTypeDefinition());
            }
        }
        
        public class NullableSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (context.Node is MemberExpression memberExpression &&
                    memberExpression.Member.Name == "Value" &&
                    memberExpression.Expression.Type.IsNullableType())
                {
                    context.PreventDefault();
                    context.Visitor.Visit(memberExpression.Expression);
                }

            }
        }

        public class CompareExchangeSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var methodCallExpression = context.Node as MethodCallExpression;

                if (methodCallExpression?.Method.Name != nameof(RavenQuery.CmpXchg))
                    return;

                if (methodCallExpression.Method.DeclaringType != typeof(RavenQuery) 
                    && (methodCallExpression.Object == null || methodCallExpression.Object.Type != typeof(IDocumentSession)))
                    return;

                var key = methodCallExpression.Arguments[0];

                if (string.IsNullOrEmpty(key.ToString()))
                {
                    throw new NotSupportedException("");                    
                }

                context.PreventDefault();
 
                var writer = context.GetWriter();
                using (writer.Operation(methodCallExpression))
                {
                    writer.Write("cmpxchg(");
                    context.Visitor.Visit(key);
                    writer.Write(")");
                }
            }
        }
        
        public class LoadSupport : JavascriptConversionExtension
        {
            public bool HasLoad { get; set; }
            public Expression Arg { get; set; }
            public bool IsEnumerable { get; set; }
            public bool DoNotTranslate { get; set; }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var methodCallExpression = context.Node as MethodCallExpression;

                if (methodCallExpression?.Method.Name != "Load")
                    return;

                if (methodCallExpression.Method.DeclaringType != typeof(RavenQuery) 
                    && (methodCallExpression.Object == null || methodCallExpression.Object.Type != typeof(IDocumentSession)))
                    return;

                HasLoad = true;
                Arg = methodCallExpression.Arguments[0];
                IsEnumerable = Arg.Type.IsArray || LinqMethodsSupport.IsCollection(Arg.Type);

                if (IsEnumerable && methodCallExpression.Object?.Type == typeof(IDocumentSession))
                {
                    throw new NotSupportedException("Using IDocumentSession.Load(IEnumerable<string> ids) inside a query is not supported. " +
                                                    "You should use RavenQuery.Load(IEnumerable<string> ids) instead")
                    {
                        HelpLink = "DoNotWrap"
                    };                    
                }

                context.PreventDefault();

                if (DoNotTranslate)
                    return;
               
                var writer = context.GetWriter();
                using (writer.Operation(methodCallExpression))
                {
                    writer.Write("load(");
                    context.Visitor.Visit(Arg);
                    writer.Write(")");
                }
                               
            }
        }

        public class MathSupport : JavascriptConversionExtension
        {
            private static readonly Dictionary<string, string> SupportedNames = new Dictionary<string, string>{
                {"Abs", "abs"}, {"Acos", "acos"}, {"Asin", "asin"}, {"Atan", "atan"}, {"Atan2", "atan2"},
                {"Ceiling", "ceil"}, {"Cos", "cos"}, {"Exp", "exp"}, {"Floor", "floor"}, {"Log", "log"},
                {"Max", "max"}, {"Min", "min" }, {"Pow", "pow" }, {"Round", "round"}, {"Sin", "sin"},
                {"Sqrt", "sqrt"}, {"Tan", "tan"}
            };

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var methodCallExpression = context.Node as MethodCallExpression;
                var method = methodCallExpression?.Method;

                if (method == null || method.DeclaringType != typeof(Math))
                    return;

                if (SupportedNames.ContainsKey(method.Name) == false)
                    throw new NotSupportedException($"Translation of System.Math.{method.Name} to JavaScript is not supported");

                var writer = context.GetWriter();
                context.PreventDefault();

                using (writer.Operation(methodCallExpression))
                {
                    writer.Write("Math.");
                    writer.Write(SupportedNames[method.Name]);

                    writer.Write("(");

                    for (var i = 0; i < methodCallExpression.Arguments.Count; i++)
                    {
                        if (i != 0)
                        {
                            writer.Write(", ");
                        }

                        context.Visitor.Visit(methodCallExpression.Arguments[i]);
                    }

                    writer.Write(")");
                }
            }
        }
        
        public class WrappedConstantSupport<T> : JavascriptConversionExtension
        {
            public IAbstractDocumentQuery<T> DocumentQuery { private get; set; }
            public List<string> ProjectionParameters { get; set; }
            
            private static bool IsWrapedConstatntExpression(Expression expression)
            {            
                while (expression is MemberExpression memberExpression)
                {
                    expression = memberExpression.Expression;
                }

                return expression is ConstantExpression;
            }
            
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is MemberExpression memberExpression) ||
                    IsWrapedConstatntExpression(memberExpression) == false)                   
                    return;
                
                LinqPathProvider.GetValueFromExpressionWithoutConversion(memberExpression, out var value);
                var parameter = DocumentQuery.ProjectionParameter(value);
                ProjectionParameters?.Add(parameter);
                
                var writer = context.GetWriter();
                context.PreventDefault();

                using (writer.Operation(memberExpression))
                {
                    writer.Write(parameter);
                }
            }
        }
              
        public class JsonPropertyAttributeSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is MemberExpression memberExpression))
                    return;
                
                var jsonPropertyAttribute = memberExpression.Member.GetCustomAttributes()
                                                                    .OfType<JsonPropertyAttribute>()
                                                                    .FirstOrDefault();
                if (jsonPropertyAttribute == null)
                    return;
                
                context.PreventDefault();
                var writer = context.GetWriter();

                using (writer.Operation(memberExpression))
                {
                    context.Visitor.Visit(memberExpression.Expression);
                    writer.Write(".");
                    writer.Write(jsonPropertyAttribute.PropertyName ?? memberExpression.Member.Name);
                }
            }
        }

        public class ReplaceParameterWithNewName : JavascriptConversionExtension
        {
            private readonly string _newName;
            private readonly ParameterExpression _parameter;

            public ReplaceParameterWithNewName(ParameterExpression parameter, string newName )
            {
                _newName = newName;
                _parameter = parameter;
            }
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var parameter = context.Node as ParameterExpression;
                if (parameter == null || parameter != _parameter)
                    return;

                context.PreventDefault();
                var writer = context.GetWriter();

                using (writer.Operation(parameter))
                {
                    writer.Write(_newName);
                }
            }
        }

        public class TransparentIdentifierSupport : JavascriptConversionExtension
        {
            public bool DoNotIgnore { get; set; }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (context.Node is LambdaExpression lambdaExpression
                    && lambdaExpression.Parameters.Count > 0 
                    && lambdaExpression.Parameters[0].Name.StartsWith("<>h__TransparentIdentifier"))
                {
                    DoNotIgnore = true;

                    context.PreventDefault();

                    var writer = context.GetWriter();
                    using (writer.Operation(lambdaExpression))
                    {
                        writer.Write("function(");
                        writer.Write(lambdaExpression.Parameters[0].Name.Substring(2));
                        writer.Write("){return ");
                        context.Visitor.Visit(lambdaExpression.Body);
                        writer.Write(";}");

                    }

                    return;
                }

                if (context.Node is ParameterExpression p && 
                    p.Name.StartsWith("<>h__TransparentIdentifier") && 
                    DoNotIgnore)
                {
                    context.PreventDefault();
                    var writer = context.GetWriter();
                    using (writer.Operation(p))
                    {
                        writer.Write(p.Name.Substring(2));
                    }
                }

                if (!(context.Node is MemberExpression member))
                    return;

                if (member.Expression is MemberExpression innerMember
                    && innerMember.Member.Name.StartsWith("<>h__TransparentIdentifier"))
                {
                    context.PreventDefault();

                    var writer = context.GetWriter();
                    using (writer.Operation(innerMember))
                    {
                        if (DoNotIgnore)
                        {
                            context.Visitor.Visit(member.Expression);
                            writer.Write(".");

                        }
                        writer.Write($"{member.Member.Name}");
                    }

                }

                if (member.Expression is ParameterExpression parameter && parameter.Name.StartsWith("<>h__TransparentIdentifier"))
                {
                    context.PreventDefault();

                    var writer = context.GetWriter();
                    using (writer.Operation(parameter))
                    {
                        var name = member.Member.Name;

                        if (DoNotIgnore)
                        {
                            writer.Write(parameter.Name.Substring(2));
                            writer.Write(".");
                            name = name.Replace("<>h__TransparentIdentifier", "h__TransparentIdentifier");
                        }

                        writer.Write($"{name}");
                    }
                }
            }
        }

        public class InvokeSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is InvocationExpression invocationExpression))
                    return;

                context.PreventDefault();
                context.Visitor.Visit(invocationExpression.Expression);

                var writer = context.GetWriter();
                using (writer.Operation(invocationExpression))
                {

                    writer.Write("(");

                    for (var i = 0; i < invocationExpression.Arguments.Count; i++)
                    {
                        if (i != 0)
                        {
                            writer.Write(", ");
                        }

                        context.Visitor.Visit(invocationExpression.Arguments[i]);
                    }

                    writer.Write(")");
                }
            }
        }

        public class ValueTypeParseSupport : JavascriptConversionExtension
        {
            private static readonly Dictionary<Type, string> ValueTypes = new Dictionary<Type, string>() {
                { typeof(int), "parseInt" }, { typeof(uint), "parseInt" }, { typeof(double), "parseFloat" }, { typeof(decimal), "parseFloat" },
                { typeof(bool), "\"true\" == " }, { typeof(char), "" }, { typeof(long), "parseInt" }, { typeof(ulong), "parseInt" },
                { typeof(sbyte), "parseInt" },  { typeof(short), "parseInt" }, {typeof(ushort), "parseInt" }, { typeof(byte), "parseInt" }
            };

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var methodCallExpression = context.Node as MethodCallExpression;
                var method = methodCallExpression?.Method;

                if (method == null || method.Name != "Parse" || 
                    !ValueTypes.TryGetValue(method.DeclaringType, out var expr))
                    return;

                context.PreventDefault();
                var writer = context.GetWriter();
                using (writer.Operation(methodCallExpression))
                {
                    writer.Write(expr);
                    writer.Write("(");
                    context.Visitor.Visit(methodCallExpression.Arguments[0]);
                    writer.Write(")");
                }
            }
        }

        public class DateTimeSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (context.Node is NewExpression newExp && newExp.Type == typeof(DateTime))
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

                if (!(context.Node is MemberExpression node))
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
                            writer.Write("new Date(Date.parse(");
                            context.Visitor.Visit(node.Expression); //visit inner expression (user ,order.ShipmentInfo, etc)
                            writer.Write($".{node.Member.Name}))");
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
                            writer.Write("Date.parse(");
                            context.Visitor.Visit(memberExpression.Expression);
                            writer.Write($".{memberExpression.Member.Name}");
                            writer.Write(")");
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

        public class ConstSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is ConstantExpression nodeAsConst))
                    return;

                var writer = context.GetWriter();
                using (writer.Operation(nodeAsConst))
                {
                    if (nodeAsConst.Value == null)
                    {
                        context.PreventDefault();
                        writer.Write("null");
                        return;
                    }

                    if (nodeAsConst.Type == typeof(bool))
                    {
                        context.PreventDefault();

                        var val = (bool)nodeAsConst.Value ? "true" : "false";

                        using (writer.Operation(nodeAsConst))
                        {
                            writer.Write(val);
                        }

                        return;
                    }

                    if (nodeAsConst.Type == typeof(char))
                    {
                        context.PreventDefault();

                        writer.Write("\"");
                        writer.Write(nodeAsConst.Value);
                        writer.Write("\"");

                        return;

                    }

                    if (nodeAsConst.Type.IsArray || LinqMethodsSupport.IsCollection(nodeAsConst.Type))
                    {
                        var arr = nodeAsConst.Value as object[] ?? (nodeAsConst.Value as IEnumerable<object>)?.ToArray();
                        if (arr == null)
                            return;

                        context.PreventDefault();

                        writer.Write("[");

                        for (var i = 0; i < arr.Length; i++)
                        {
                            if (i != 0)
                                writer.Write(", ");
                            if (arr[i] is string || arr[i] is char)
                            {
                                writer.Write("\"");
                                writer.Write(arr[i]);
                                writer.Write("\"");
                            }

                            else
                            {
                                writer.Write(arr[i]);
                            }

                        }
                        writer.Write("]");

                    }
                }
            }
        }

        public class NullCoalescingSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (context.Node.NodeType != ExpressionType.Coalesce || !(context.Node is BinaryExpression binaryExpression))
                    return;

                context.PreventDefault();
                var writer = context.GetWriter();

                using (writer.Operation(binaryExpression))
                {
                    context.Visitor.Visit(binaryExpression.Left);
                    writer.Write(" != null ? ");
                    context.Visitor.Visit(binaryExpression.Left);
                    writer.Write(" : ");
                    context.Visitor.Visit(binaryExpression.Right);
                }
            }
        }


        public class ListInitSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (context.Node is ListInitExpression lie && 
                    LinqMethodsSupport.IsCollection(lie.Type) && 
                    LinqMethodsSupport.IsDictionary(lie.Type) == false)
                {
                    context.PreventDefault();
                    var writer = context.GetWriter();
                    using (writer.Operation(0))
                    {
                        writer.Write('[');

                        var posStart = writer.Length;
                        foreach (var init in lie.Initializers)
                        {
                            if (writer.Length > posStart)
                                writer.Write(',');

                            if (init.Arguments.Count != 1)
                                throw new Exception(
                                    "Arrays can only be initialized with methods that receive a single parameter for the value");

                            context.Visitor.Visit(init.Arguments[0]);
                        }

                        writer.Write(']');
                    }
                }
            }
        }

        public class NewSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (context.Node is NewExpression newExp)
                {
                    if (newExp.Members != null && newExp.Members.Count > 0)
                    {
                        context.PreventDefault();
                        var resultWriter = context.GetWriter();

                        using (resultWriter.Operation(0))
                        {
                            resultWriter.Write('{');

                            var posStart = resultWriter.Length;
                            for (int itMember = 0; itMember < newExp.Members.Count; itMember++)
                            {
                                var member = newExp.Members[itMember];

                                if (resultWriter.Length > posStart)
                                    resultWriter.Write(',');

                                string name = member.Name;
                                if (member.Name.StartsWith("<>h__TransparentIdentifier"))
                                {
                                    name = name.Substring(2);
                                }

                                if (Regex.IsMatch(name, @"^\w[\d\w]*$"))
                                    resultWriter.Write(name);
                                else
                                    WriteStringLiteral(name, resultWriter);

                                resultWriter.Write(':');
                                context.Visitor.Visit(newExp.Arguments[itMember]);
                            }

                            resultWriter.Write('}');
                        }
                    }

                    if (LinqMethodsSupport.IsCollection(newExp.Type) && 
                        LinqMethodsSupport.IsDictionary(newExp.Type) == false)
                    {
                        var writer = context.GetWriter();
                        context.PreventDefault();

                        if (newExp.Arguments.Count > 0 && 
                            LinqMethodsSupport.IsCollection(newExp.Arguments[0].Type))
                        {
                            context.Visitor.Visit(newExp.Arguments);
                            return;
                        }

                        using (writer.Operation(newExp))
                        {
                            writer.Write("[]");
                        }

                        return;
                    }

                    if (newExp.Arguments.Count == 0)
                    {
                        var writer = context.GetWriter();
                        context.PreventDefault();

                        using (writer.Operation(newExp))
                        {
                            writer.Write("{}");
                        }

                        return;
                    }
                }

                if (context.Node is NewArrayExpression nae)
                {
                    var writer = context.GetWriter();
                    context.PreventDefault();

                    using (writer.Operation(nae))
                    {
                        writer.Write("[");

                        if (nae.Expressions.Count == 1 && 
                            nae.Expressions[0] is ConstantExpression val && 
                            val.Value is int asInt )
                        {
                            if (asInt != 0)
                            {
                                var elementsType = nae.Type.GetElementType();
                                var defaultVal = LinqMethodsSupport.GetDefault(elementsType);

                                if (defaultVal is bool)
                                {
                                    defaultVal = "false";
                                }
                                else if (defaultVal is char)
                                {
                                    defaultVal = '0';
                                }

                                for (var i = 0; i < asInt; i++)
                                {
                                    writer.Write(defaultVal ?? "null");
                                    if (i < asInt - 1)
                                        writer.Write(",");
                                }
                            }                                               
                        }

                        else if (nae.Expressions.Count > 0)
                        {
                            for (var i = 0; i < nae.Expressions.Count; i++)
                            {
                                if (i != 0)
                                    writer.Write(",");
                                context.Visitor.Visit(nae.Expressions[i]);
                            }
                        }

                        writer.Write("]");
                    }

                    return;
                }

                if (context.Node is MethodCallExpression mce &&
                    mce.Method.DeclaringType == typeof(Array) &&
                    mce.Method.Name == "Empty")
                {
                    var writer = context.GetWriter();
                    context.PreventDefault();

                    using (writer.Operation(mce))
                    {
                        writer.Write("[]");
                    }
                }
            }

            private void WriteStringLiteral(string str, JavascriptWriter writer)
            {
                writer.Write('"');
                writer.Write(
                    str
                        .Replace("\\", "\\\\")
                        .Replace("\r", "\\r")
                        .Replace("\n", "\\n")
                        .Replace("\t", "\\t")
                        .Replace("\0", "\\0")
                        .Replace("\"", "\\\""));

                writer.Write('"');
            }

        }


        public class NullComparisonSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {                
                if (!(context.Node is BinaryExpression binaryExpression) || 
                    (binaryExpression.NodeType != ExpressionType.Equal && 
                     binaryExpression.NodeType != ExpressionType.NotEqual)) 
                    return;
                
                if (binaryExpression.Right is ConstantExpression right && right.Value == null)
                {
                    if (binaryExpression.Left is ConstantExpression leftConst && leftConst.Value == null)
                        return;

                    WriteExpression(context, binaryExpression.Left, binaryExpression.NodeType);
                    return;
                }

                if (binaryExpression.Left is ConstantExpression left && left.Value == null)
                {
                    WriteExpression(context, binaryExpression.Right, binaryExpression.NodeType);
                }

            }

            private static void WriteExpression(JavascriptConversionContext context, Expression expression, ExpressionType op)
            {
                var writer = context.GetWriter();                
                context.PreventDefault();
                
                context.Visitor.Visit(expression);                
                writer.Write(op == ExpressionType.Equal ? "==null" : "!=null");                
            }
        }

        public class NestedConditionalSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var cond = context.Node as ConditionalExpression;
                if (cond?.IfTrue is ConditionalExpression || cond?.IfFalse is ConditionalExpression == false)
                    return;

                var writer = context.GetWriter();
                context.PreventDefault();

                using (writer.Operation(cond))
                {
                    context.Visitor.Visit(cond.Test);
                    writer.Write(" ? ");
                    context.Visitor.Visit(cond.IfTrue);
                    writer.Write(" : ");
                    context.Visitor.Visit(cond.IfFalse);
                }
            }
        }

        public class StringSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is MethodCallExpression mce) || mce.Method.DeclaringType != typeof(string))
                    return;

                string newName;
                switch (mce.Method.Name)
                {
                    case "PadLeft":
                        newName = "padStart";
                        break;
                    case "PadRight":
                        newName = "padEnd";
                        break;
                    case "Substring":
                        newName = "substr";
                        break;
                    case "StartsWith":
                        newName = "startsWith";
                        break;
                    case "EndsWith":
                        newName = "endsWith";
                        break;
                    case "Join":
                        newName = "join";
                        break;
                    case "Contains":
                        newName = "indexOf";
                        break;
                    case "Split":
                        newName = "split";
                        break;
                    case "Trim":
                        newName = "trim";
                        break;
                    case "ToUpper":
                        newName = "toUpperCase";
                        break;
                    case "ToLower":
                        newName = "toLowerCase";
                        break;
                    case "Replace":
                        newName = "replace";
                        break;
                    case "IsNullOrEmpty":
                        newName = "nullOrEmpty";
                        break;
                    case "IsNullOrWhiteSpace":
                        newName = "nullOrWhitespace";
                        break;
                    case "ToCharArray":
                        newName = "toCharArray";
                        break;
                    default:
                        return;
                }

                var writer = context.GetWriter();
                context.PreventDefault();

                using (writer.Operation(mce))
                {
                    switch (newName)
                    {
                        case "join":
                            if (mce.Arguments.Count > 2)
                            {
                                writer.Write("[");

                                for (int i = 1; i < mce.Arguments.Count; i++)
                                {
                                    if (i != 1)
                                    {
                                        writer.Write(", ");
                                    }
                                    context.Visitor.Visit(mce.Arguments[i]);
                                }

                                writer.Write("]");
                            }
                            else
                            {
                                context.Visitor.Visit(mce.Arguments[1]);
                            }

                            writer.Write($".{newName}(");
                            context.Visitor.Visit(mce.Arguments[0]);
                            writer.Write(")");
                            break;
                        case "nullOrEmpty":
                            writer.Write("(");
                            context.Visitor.Visit(mce.Arguments[0]);
                            writer.Write(" == null || ");
                            context.Visitor.Visit(mce.Arguments[0]);
                            writer.Write(" === \"\")");
                            break;
                        case "nullOrWhitespace":
                            writer.Write("(!");
                            context.Visitor.Visit(mce.Arguments[0]);
                            writer.Write(" || !");
                            context.Visitor.Visit(mce.Arguments[0]);
                            writer.Write(".trim())");
                            break;
                        case "toCharArray":
                            context.Visitor.Visit(mce.Object);
                            if (mce.Arguments.Count > 0)
                            {
                                writer.Write(".substr(");
                                context.Visitor.Visit(mce.Arguments[0]);
                                writer.Write(", ");
                                context.Visitor.Visit(mce.Arguments[1]);
                                writer.Write(")");
                            }
                            writer.Write(".split('')");
                            break;
                        default:
                            context.Visitor.Visit(mce.Object);
                            writer.Write($".{newName}(");

                            if (newName == "split")
                            {
                                writer.Write("new RegExp(");
                                if (mce.Arguments[0] is NewArrayExpression arrayExpression)
                                {
                                    for (var i = 0; i < arrayExpression.Expressions.Count; i++)
                                    {
                                        if (i != 0)
                                        {
                                            writer.Write("+\"|\"+");
                                        }

                                        context.Visitor.Visit(arrayExpression.Expressions[i]);
                                    }
                                }
                                else if(mce.Arguments[0] is MethodCallExpression mce2)
                                {
                                    var value = Expression.Lambda(mce2).Compile().DynamicInvoke();
                                    switch (value)
                                    {
                                        case string s:
                                            writer.WriteLiteral(s);
                                            break;
                                        case Array items:
                                            for (var i = 0; i < items.Length; i++)
                                            {
                                                if (i != 0)
                                                {
                                                    writer.Write("+\"|\"+");
                                                }

                                                var str = items.GetValue(i).ToInvariantString();

                                                writer.WriteLiteral(str);
                                            }
                                            break;
                                        default:
                                            throw new InvalidOperationException("Unable to understand how to convert " + value + " to RQL (" + value?.GetType() ?? "null" +")");
                                    }
                                }
                                else
                                {
                                    context.Visitor.Visit(mce.Arguments[0]);
                                }

                                writer.Write(", \"g\")");
                            }
                            else if (newName == "replace")
                            {
                                writer.Write("new RegExp(");
                                context.Visitor.Visit(mce.Arguments[0]);
                                writer.Write(", \"g\"), ");

                                context.Visitor.Visit(mce.Arguments[1]);
                            }
                            else
                            {
                                for (var i = 0; i < mce.Arguments.Count; i++)
                                {
                                    if (i != 0)
                                    {
                                        writer.Write(", ");
                                    }

                                    context.Visitor.Visit(mce.Arguments[i]);
                                }
                            }

                            writer.Write(")");

                            if (mce.Method.Name == "Contains")
                            {
                                writer.Write(" !== -1");
                            }
                            break;
                    }
                }
            }
        }

        public class MetadataSupport : JavascriptConversionExtension
        {
            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var methodCallExpression = context.Node as MethodCallExpression;

                if (methodCallExpression?.Method.Name != "Metadata"
                    && methodCallExpression?.Method.Name != "GetMetadataFor")
                    return;

                if (methodCallExpression.Method.DeclaringType != typeof(RavenQuery)
                    && methodCallExpression.Object?.Type != typeof(IAdvancedSessionOperations)
                    && methodCallExpression.Object?.Type != typeof(IAsyncAdvancedSessionOperations))
                    return;

                context.PreventDefault();
                var writer = context.GetWriter();
                using (writer.Operation(methodCallExpression))
                {
                    writer.Write("getMetadata(");
                    context.Visitor.Visit(methodCallExpression.Arguments[0]);
                    writer.Write(")");
                }
            }
        }

        public class IdentityPropertySupport : JavascriptConversionExtension
        {
            public Dictionary<string, string> AliasesToIdProperty { get; set; }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {                
                if (!(context.Node is MemberExpression member))
                    return;

                if (member.Expression is ParameterExpression parameter 
                    && AliasesToIdProperty.TryGetValue(parameter.Name , out var id) 
                    && id == member.Member.Name)
                {
                    var writer = context.GetWriter();
                    context.PreventDefault();

                    using (writer.Operation(member))
                    {
                        writer.Write($"id({parameter.Name})");
                    }
                    return;
                }

                if (!(member.Expression is MemberExpression innerMember))
                    return;

                var p = GetParameter(innerMember);

                if (p != null && p.StartsWith("<>h__TransparentIdentifier") 
                    && AliasesToIdProperty.TryGetValue(innerMember.Member.Name, out id)
                    && id == member.Member.Name)
                {
                    var writer = context.GetWriter();
                    context.PreventDefault();

                    using (writer.Operation(member))
                    {
                        writer.Write("id(");
                        context.Visitor.Visit(innerMember);
                        writer.Write(")");
                    }
                }
            }

            private string GetParameter(MemberExpression expression)
            {
                while (expression.Expression is MemberExpression memberExpression)
                {
                    expression = memberExpression;
                }

                return (expression.Expression as ParameterExpression)?.Name;
            }
        }

    }
}
