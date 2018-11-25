using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Lambda2Js;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Sparrow.Extensions;

namespace Raven.Client.Util
{
    internal class JavascriptConversionExtensions
    {
        internal const string TransparentIdentifier = "<>h__TransparentIdentifier";
        private const string DefaultAliasPrefix = "__rvn";

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
                foreach (var expression in methodCallExpression.Arguments)
                {
                    if (expression.Type.IsArray == false)
                    {
                        args.Add(expression);
                        continue;
                    }

                    if (expression is NewArrayExpression newArrayExpression)
                    {
                        args.AddRange(newArrayExpression.Expressions);
                        continue;
                    }

                    if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression, out var arrayValue) == false)
                    {
                        throw new InvalidOperationException($"Couldn't get value from an array expression: {expression}");
                    }

                    var array = arrayValue as Array;
                    Debug.Assert(array != null);

                    foreach (var value in array)
                    {
                        args.Add(Expression.Constant(value));
                    }
                }

                if (nameAttribute.Name == "filter")
                {
                    if (args[0] is LambdaExpression lambda)
                    {
                        javascriptWriter.Write(lambda.Parameters[0].Name);
                        javascriptWriter.Write(" => ");
                        javascriptWriter.Write(" !("); // negate body
                        context.Visitor.Visit(lambda.Body);
                        javascriptWriter.Write(")");
                    }
                }
                else
                {
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

        public class DictionarySupport : JavascriptConversionExtension
        {
            public enum DictionaryInnerCall
            {
                None,
                Key,
                Value,
                KeyValue,
                Map
            }

            private DictionaryInnerCall _innerCallExpected;

            private string _paramName = string.Empty;

            private void HandleMap(JavascriptConversionContext context, MethodCallExpression mce)
            {
                if (mce.Arguments.Count < 2 || !(mce.Arguments[1] is LambdaExpression lambda))
                    return;

                _paramName = lambda.Parameters[0]?.Name;

                context.PreventDefault();
                var writer = context.GetWriter();
                using (writer.Operation(lambda))
                {
                    writer.Write("Object.map(");
                    context.Visitor.Visit(mce.Arguments[0]);
                    writer.Write(", function(v, k){ return ");
                    context.Visitor.Visit(lambda.Body);
                    writer.Write(";})");

                    _innerCallExpected = default;
                    _paramName = string.Empty;
                }
            }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                // Rewrite .Count / .Values / .Keys
                if (context.Node is MemberExpression contextNode
                    && contextNode.Expression != null
                    && typeof(IDictionary).IsAssignableFrom(contextNode.Expression.Type))
                {
                    context.PreventDefault();
                    // Get KeyValueType identifier:
                    var keyValuePairType = typeof(KeyValuePair<,>)
                                .MakeGenericType(contextNode.Expression.Type.GetGenericArguments());

                    switch (contextNode.Member.Name)
                    {
                        case "Count":
                        {
                            var expression = Expression.Call(
                                typeof(Enumerable),
                                "Count",
                                new[] { keyValuePairType },
                                contextNode.Expression
                            );
                            _innerCallExpected = DictionaryInnerCall.Key;
                            context.Visitor.Visit(expression);
                            return;
                        }
                        case "Keys":
                            _innerCallExpected = DictionaryInnerCall.Key;
                            context.Visitor.Visit(contextNode.Expression);
                            return;
                        case "Values":
                            _innerCallExpected = DictionaryInnerCall.Value;
                            context.Visitor.Visit(contextNode.Expression);
                            return;
                    }
                }

                // Only call it when we do a methodCall on a memberExpression of type dictionary
                if (context.Node is MethodCallExpression callNode
                    && callNode.Arguments.Count > 0
                    && typeof(IDictionary).IsAssignableFrom(callNode.Arguments[0].Type))
                {
                    if (_innerCallExpected == default)
                    {
                        // If not given, decide on method name if we should shorten:
                        switch (callNode.Method.Name)
                        {
                            default:
                                _innerCallExpected = DictionaryInnerCall.KeyValue;
                                break;
                            case "Count":
                                _innerCallExpected = DictionaryInnerCall.Key;
                                break;
                            case "Select":
                                _innerCallExpected = DictionaryInnerCall.Map;
                                HandleMap(context, callNode);
                                return;
                        }
                    }
                }

                // Now we translate the memberExpression
                else if (_innerCallExpected != default
                    && _innerCallExpected != DictionaryInnerCall.Map
                    && typeof(IDictionary).IsAssignableFrom(context.Node.Type))
                {
                    context.PreventDefault();
                    var currentCall = _innerCallExpected;
                    _innerCallExpected = default;

                    var writer = context.GetWriter();
                    using (writer.Operation(context.Node))
                    {
                        writer.Write("Object.keys(");
                        context.Visitor.Visit(context.Node);
                        writer.Write(")");

                        // Do not translate Key (we already have the keys here!)
                        if (currentCall != DictionaryInnerCall.Key)
                        {
                            writer.Write(".map(function(a){");

                            switch (currentCall)
                            {
                                case DictionaryInnerCall.Value:
                                    writer.Write("return ");
                                    context.Visitor.Visit(context.Node);
                                    writer.Write("[a];");
                                    break;
                                case DictionaryInnerCall.KeyValue:
                                    writer.Write("return{Key: a,Value:");
                                    context.Visitor.Visit(context.Node);
                                    writer.Write("[a]};");
                                    break;
                            }

                            writer.Write("})");
                        }
                    }
                }

                if (_innerCallExpected == DictionaryInnerCall.Map 
                    && context.Node is MemberExpression memberExpression
                    && memberExpression.Member.Name.In("Value", "Key"))
                {
                    var p = GetParameter(memberExpression);

                    if (p?.Name == _paramName 
                        && p?.Type.GenericTypeArguments.Length > 0  
                        && p.Type.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))
                    {
                        context.PreventDefault();
                        var writer = context.GetWriter();
                        using (writer.Operation(memberExpression))
                        {
                            writer.Write(memberExpression.Member.Name == "Value" ? "v" : "k");
                        }                      
                    }
                }
            }
        }

        public class LinqMethodsSupport : JavascriptConversionExtension
        {
            public static readonly LinqMethodsSupport Instance = new LinqMethodsSupport();

            private LinqMethodsSupport()
            {
            }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (context.Node is MemberExpression node && node.Member.Name == "Count" && IsCollection(node.Member.DeclaringType))
                {
                    HandleCount(context, node.Expression);
                    return;
                }

                var methodCallExpression = context.Node as MethodCallExpression;
                var method = methodCallExpression?.Method;
                if (method == null || method.IsSpecialName)
                    return;

                var methodName = method.Name;
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
                                
                                writer.Write(".length > 0");
                                return;                           
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
                    case "Cast":
                    case "ToList":
                    case "ToArray":
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        return;
                    }

                    case "Concat":
                        newName = "concat";
                        break;

                    case "Average":
                    {
                        context.PreventDefault();
                        // -- Rewrite expression to Sum() (using second (if available) argument Types) / Count() (using last argument Type)

                        var sum = Expression.Call(
                            typeof(Enumerable),
                            "Sum",
                            methodCallExpression.Arguments.Count > 1 ?
                                new Type[] { methodCallExpression.Arguments[1].Type.GenericTypeArguments.First() } :
                                new Type[] { },
                            methodCallExpression.Arguments.ToArray());

                        // Get resulting type by interface of IEnumerable<>
                        var typeArguments = methodCallExpression.Arguments[0].Type.GetInterfaces()
                            .Concat(new[] { methodCallExpression.Arguments[0].Type })
                            .First(a => a.GetTypeInfo().IsGenericType && a.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                            .GetGenericArguments()
                            .First();
                        var count = Expression.Call(typeof(Enumerable), "Count", new Type[] { typeArguments }, methodCallExpression.Arguments[0]);

                        // When doing the divide, make sure count matches the sum type

                        context.Visitor.Visit(sum);

                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            writer.Write("/(");
                            context.Visitor.Visit(Expression.Convert(count, sum.Type));

                            // Avoid division by 0                            
                            writer.Write("||1)");
                            return;
                        }
                    }
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
                            HandleMaxOrMin(context, methodCallExpression, true);
                            return;
                        }
                    case "Min":
                        {
                            HandleMaxOrMin(context, methodCallExpression);
                            return;
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
                                writer.Write($"(function(arr){{return arr.length > 0 ? arr : [{defaultVal ?? "null"}]}})(");
                                context.Visitor.Visit(methodCallExpression.Arguments[0]);
                                writer.Write(")");
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
                    {
                        context.PreventDefault();
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        var writer = context.GetWriter();
                        using (writer.Operation(methodCallExpression))
                        {
                            if (methodCallExpression.Arguments.Count > 1)
                            {
                                writer.Write(".filter");
                                context.Visitor.Visit(methodCallExpression.Arguments[1]);
                            }

                            writer.Write(".length");
                            return;
                        }
                    }
                    default:
                        throw new NotSupportedException($"Unable to translate '{methodName}' to RQL operation because this method is not familiar to the RavenDB query provider.")
                        {
                            HelpLink = "DoNotWrap"
                        };
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
                    context.Visitor.Visit(methodCallExpression.Arguments[0]);

                    // When having no other arguments, don't call the function, when it's a .map operation
                    // .Sum()/.Average()/.Select() for example can be called without arguments, which means, 
                    // map all, though .map() without arguments returns an empty list
                    if (newName != "map" || methodCallExpression.Arguments.Count > 1)
                    {
                        javascriptWriter.Write($".{newName}");
                        javascriptWriter.Write("(");
                        if (methodCallExpression.Arguments.Count > 1)
                        {
                            context.Visitor.Visit(methodCallExpression.Arguments[1]);
                        }
                        javascriptWriter.Write(")");
                    }
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

            private static void HandleMaxOrMin(JavascriptConversionContext context, MethodCallExpression methodCallExpression, bool max = false)
            {
                context.PreventDefault();
                context.Visitor.Visit(methodCallExpression.Arguments[0]);

                var maxOrMin = max ? "Raven_Max" : "Raven_Min";
                var writer = context.GetWriter();
                using (writer.Operation(methodCallExpression))
                {
                    if (methodCallExpression.Arguments.Count > 1)
                    {
                        writer.Write(".map");
                        context.Visitor.Visit(methodCallExpression.Arguments[1]);
                    }

                    writer.Write($".reduce(function(a, b) {{ return {maxOrMin}(a, b);}}");

                    var defaultVal = GetDefault(methodCallExpression.Type);
                    if (defaultVal != null)
                    {
                        writer.Write($", {defaultVal}");
                    }
                    writer.Write(")");

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
            public static readonly NullableSupport Instance = new NullableSupport();

            private NullableSupport()
            {
            }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is MemberExpression memberExpression) || 
                    memberExpression.Expression.Type.IsNullableType() == false)
                    return;

                if (memberExpression.Member.Name == "Value")
                {
                    context.PreventDefault();
                    context.Visitor.Visit(memberExpression.Expression);
                }
                else if (memberExpression.Member.Name == "HasValue")
                {
                    context.PreventDefault();
                    var writer = context.GetWriter();
                    using (writer.Operation(memberExpression))
                    {
                        context.Visitor.Visit(memberExpression.Expression);
                        writer.Write(" != null");
                    }
                }

            }
        }

        public class CompareExchangeSupport : JavascriptConversionExtension
        {
            public static readonly CompareExchangeSupport Instance = new CompareExchangeSupport();

            private CompareExchangeSupport()
            {
            }

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

        public class CounterSupport : JavascriptConversionExtension
        {
            public static readonly CounterSupport Instance = new CounterSupport();

            private CounterSupport()
            {
            }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is MethodCallExpression mce))
                    return;

                if (LinqPathProvider.IsCounterCall(mce) == false)
                    return;

                Expression alias;
                int nameIndex;

                if (mce.Method.DeclaringType != typeof(RavenQuery))
                {
                    alias = (mce.Object as MethodCallExpression)?.Arguments[0];
                    nameIndex = 0;
                }
                else
                {
                    alias = mce.Arguments[0];
                    nameIndex = 1;
                }

                context.PreventDefault();

                var writer = context.GetWriter();
                using (writer.Operation(mce))
                {
                    writer.Write("counter(");
                    context.Visitor.Visit(alias);
                    writer.Write(", ");
                    context.Visitor.Visit(mce.Arguments[nameIndex]);
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
            public static readonly MathSupport Instance = new MathSupport();

            private static readonly Dictionary<string, string> SupportedNames = new Dictionary<string, string>{
                {"Abs", "abs"}, {"Acos", "acos"}, {"Asin", "asin"}, {"Atan", "atan"}, {"Atan2", "atan2"},
                {"Ceiling", "ceil"}, {"Cos", "cos"}, {"Exp", "exp"}, {"Floor", "floor"}, {"Log", "log"},
                {"Max", "max"}, {"Min", "min" }, {"Pow", "pow" }, {"Round", "round"}, {"Sin", "sin"},
                {"Sqrt", "sqrt"}, {"Tan", "tan"}
            };

            private MathSupport()
            {
            }

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

                    if (method.Name == "Round" && methodCallExpression.Arguments.Count > 1)
                    {
                        context.Visitor.Visit(methodCallExpression.Arguments[0]);
                        writer.Write(" * Math.pow(10, ");
                        context.Visitor.Visit(methodCallExpression.Arguments[1]);
                        writer.Write(")) / Math.pow(10, ");
                        context.Visitor.Visit(methodCallExpression.Arguments[1]);
                    }
                    else
                    {
                        for (var i = 0; i < methodCallExpression.Arguments.Count; i++)
                        {
                            if (i != 0)
                            {
                                writer.Write(", ");
                            }

                            context.Visitor.Visit(methodCallExpression.Arguments[i]);
                        }
                    }

                    writer.Write(")");
                }
            }
        }

        public class ReservedWordsSupport : JavascriptConversionExtension
        {
            public static readonly ReservedWordsSupport Instance = new ReservedWordsSupport();

            public static readonly List<string> JsReservedWords = new List<string>{
                "function", "debugger", "delete", "export","extends",
                "import", "instanceof", "super","var", "with", "load"
            };

            private ReservedWordsSupport()
            {
            }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is ParameterExpression parameterExpression) || JsReservedWords.Contains(parameterExpression.Name) == false)
                    return;

                var writer = context.GetWriter();
                context.PreventDefault();

                using (writer.Operation(parameterExpression))
                {
                    writer.Write("_");
                    writer.Write(parameterExpression);
                }
            }
        }

        public class WrappedConstantSupport<T> : JavascriptConversionExtension
        {
            private readonly IAbstractDocumentQuery<T> _documentQuery;
            private readonly List<string> _projectionParameters;
            private readonly DateTimeSupport _dateTimeSupportExtension;


            public WrappedConstantSupport(IAbstractDocumentQuery<T> documentQuery, List<string> projectionParameters, DateTimeSupport dateTimeSupportExtension)
            {
                _documentQuery = documentQuery;
                _projectionParameters = projectionParameters;
                _dateTimeSupportExtension = dateTimeSupportExtension;
            }

            private static bool IsWrappedConstantExpression(Expression expression)
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
                    IsWrappedConstantExpression(memberExpression) == false)
                    return;

                LinqPathProvider.GetValueFromExpressionWithoutConversion(memberExpression, out var value);
                var parameter = _documentQuery.ProjectionParameter(value);
                _projectionParameters?.Add(parameter);

                var writer = context.GetWriter();
                context.PreventDefault();

                using (writer.Operation(memberExpression))
                {
                    writer.Write(memberExpression.Type == typeof(DateTime) &&
                                 _dateTimeSupportExtension.InsideBinaryOperationOnDates
                        ? $"new Date(Date.parse({parameter}))"
                        : parameter);
                }
            }
        }

        public class JsonPropertyAttributeSupport : JavascriptConversionExtension
        {
            public static readonly JsonPropertyAttributeSupport Instance = new JsonPropertyAttributeSupport();

            private JsonPropertyAttributeSupport()
            {
            }

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

            public ReplaceParameterWithNewName(ParameterExpression parameter, string newName)
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
                    && lambdaExpression.Parameters[0].Name.StartsWith(TransparentIdentifier))
                {
                    DoNotIgnore = true;

                    context.PreventDefault();

                    var writer = context.GetWriter();
                    using (writer.Operation(lambdaExpression))
                    {
                        writer.Write("function(");
                        writer.Write(lambdaExpression.Parameters[0].Name.Replace(TransparentIdentifier, DefaultAliasPrefix));
                        writer.Write("){return ");
                        context.Visitor.Visit(lambdaExpression.Body);
                        writer.Write(";}");

                    }

                    return;
                }

                if (context.Node is ParameterExpression p &&
                    p.Name.StartsWith(TransparentIdentifier) &&
                    DoNotIgnore)
                {
                    context.PreventDefault();
                    var writer = context.GetWriter();
                    using (writer.Operation(p))
                    {
                        writer.Write(p.Name.Replace(TransparentIdentifier, DefaultAliasPrefix));
                    }
                }

                if (!(context.Node is MemberExpression member))
                    return;

                if (member.Expression is MemberExpression innerMember
                    && innerMember.Member.Name.StartsWith(TransparentIdentifier))
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

                        var name = member.Member.Name;

                        if (DoNotIgnore && name.StartsWith(TransparentIdentifier))
                        {
                            name = name.Replace(TransparentIdentifier, DefaultAliasPrefix);
                        }
                        else if (ReservedWordsSupport.JsReservedWords.Contains(name))
                        {
                            name = "_" + name;
                        }

                        writer.Write(name);
                    }

                }

                if (member.Expression is ParameterExpression parameter && parameter.Name.StartsWith(TransparentIdentifier))
                {
                    context.PreventDefault();

                    var writer = context.GetWriter();
                    using (writer.Operation(parameter))
                    {
                        var name = member.Member.Name;

                        if (DoNotIgnore)
                        {
                            writer.Write(parameter.Name.Replace(TransparentIdentifier, DefaultAliasPrefix));
                            writer.Write(".");
                            name = name.Replace(TransparentIdentifier, DefaultAliasPrefix);
                        }

                        if (ReservedWordsSupport.JsReservedWords.Contains(name))
                        {
                            name = "_" + name;
                        }

                        writer.Write($"{name}");
                    }
                }
            }
        }

        public class InvokeSupport : JavascriptConversionExtension
        {
            public static readonly InvokeSupport Instance = new InvokeSupport();

            private InvokeSupport()
            {

            }

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
            public static readonly ValueTypeParseSupport Instance = new ValueTypeParseSupport();

            private static readonly Dictionary<Type, string> ValueTypes = new Dictionary<Type, string>() {
                { typeof(int), "parseInt" }, { typeof(uint), "parseInt" }, { typeof(double), "parseFloat" }, { typeof(decimal), "parseFloat" },
                { typeof(bool), "\"true\" == " }, { typeof(char), "" }, { typeof(long), "parseInt" }, { typeof(ulong), "parseInt" },
                { typeof(sbyte), "parseInt" },  { typeof(short), "parseInt" }, {typeof(ushort), "parseInt" }, { typeof(byte), "parseInt" }
            };

            private ValueTypeParseSupport()
            {
            }

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

        public class ToStringSupport : JavascriptConversionExtension
        {
            public static readonly ToStringSupport Instance = new ToStringSupport();

            private ToStringSupport()
            {                
            }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is MethodCallExpression mce) || 
                    mce.Method.Name != "ToString")
                    return;

                context.PreventDefault();

                var writer = context.GetWriter();
                using (writer.Operation(mce))
                {
                    if (mce.Arguments.Count == 0)
                    {
                        context.Visitor.Visit(mce.Object);
                        writer.Write(".toString()");
                        return;
                    }

                    writer.Write("toStringWithFormat(");
                    context.Visitor.Visit(mce.Object);

                    foreach (var arg in mce.Arguments)
                    {

                        if (arg.Type == typeof(CultureInfo) &&
                            LinqPathProvider.GetValueFromExpressionWithoutConversion(arg, out var obj) &&
                            obj is CultureInfo culture)
                        {
                            if (culture.Name == string.Empty)
                                continue;
                            writer.Write(", ");
                            writer.Write($"\"{culture.Name}\"");
                        }
                        else
                        {
                            writer.Write(", ");
                            context.Visitor.Visit(arg);
                        }
                    }

                    writer.Write(")");
                }
            }
        }

        public class DateTimeSupport : JavascriptConversionExtension
        {
            public bool InsideBinaryOperationOnDates;

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

                if (context.Node is BinaryExpression binaryExpression &&
                    binaryExpression.Left.Type == typeof(DateTime))
                {
                    InsideBinaryOperationOnDates = true;

                    var writer = context.GetWriter();
                    context.PreventDefault();

                    if (context.Node.Type == typeof(TimeSpan) && 
                        context.Node.NodeType == ExpressionType.Subtract)
                    {
                        using (writer.Operation(context.Node))
                        {
                            writer.Write("convertJsTimeToTimeSpanString(");

                            context.Visitor.Visit(binaryExpression.Left);
                            writer.Write("-");

                            context.Visitor.Visit(binaryExpression.Right);

                            writer.Write(")");

                        }

                        InsideBinaryOperationOnDates = false;
                        return;
                    }

                    using (writer.Operation(context.Node))
                    {
                        context.Visitor.Visit(binaryExpression.Left);
                        writer.WriteOperator(binaryExpression.NodeType, binaryExpression.Type);
                        context.Visitor.Visit(binaryExpression.Right);
                    }

                    InsideBinaryOperationOnDates = false;
                    return;                   
                }

                if (!(context.Node is MemberExpression node))
                    return;

                if (node.Type == typeof(DateTime) && 
                    (node.Expression == null || InsideBinaryOperationOnDates))
                {
                    var writer = context.GetWriter();
                    context.PreventDefault();

                    using (writer.Operation(node))
                    {
                        if (node.Expression != null)
                        {
                            //inside a binary operation on dates, translate to JS date object

                            writer.Write("new Date(Date.parse(");
                            context.Visitor.Visit(node.Expression);
                            writer.Write($".{node.Member.Name}))");

                            return;
                        }

                        //match DateTime.Now , DateTime.UtcNow, DateTime.Today
                        switch (node.Member.Name)
                        {
                            case "MinValue":
                                writer.Write("new Date(-62135596800000)");
                                break;
                            case "MaxValue":
                                writer.Write("new Date(253402297199999)");
                                break;
                            case "Now":
                                writer.Write("new Date(Date.now())");
                                break;
                            case "UtcNow":
                                writer.Write(
                                    @"(function (date) { return new Date(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours(), date.getUTCMinutes(), date.getUTCSeconds(), date.getUTCMilliseconds());})(new Date())");
                                break;
                            case "Today":
                                writer.Write("new Date(new Date().setHours(0,0,0,0))");
                                break;
                        }
                    }

                    return;
                }

                if (node.Expression?.Type == typeof(DateTime) && node.Expression is MemberExpression memberExpression)
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

        public class SubscriptionsWrappedConstantSupport : JavascriptConversionExtension
        {            
            private readonly DocumentConventions _conventions;

            public SubscriptionsWrappedConstantSupport(DocumentConventions conventions)
            {
                _conventions = conventions;
            }

            private static bool IsWrappedConstantExpression(Expression expression)
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
                    IsWrappedConstantExpression(memberExpression) == false)
                    return;

                LinqPathProvider.GetValueFromExpressionWithoutConversion(memberExpression, out var value);
                var writer = context.GetWriter();
                using (writer.Operation(JavascriptOperationTypes.Literal))
                {
                    if (value == null)
                    {
                        context.PreventDefault();
                        writer.Write("null");
                        return;
                    }
                    
                    Type valueType = value.GetType();

                    if (memberExpression.Type == typeof(DateTime))
                    {
                        context.PreventDefault();
                        var dateTime = (DateTime)value;
                        writer.Write("new Date(Date.parse(\"");
                        writer.Write(dateTime.GetDefaultRavenFormat(isUtc: dateTime.Kind == DateTimeKind.Utc));
                        writer.Write("\"))");
                        return;
                    }

                    // Type.IsEnum is unavailable in .netstandard1.3
                    if (valueType.GetTypeInfo().IsEnum)
                    {
                        context.PreventDefault();
                        if (_conventions.SaveEnumsAsIntegers == false)
                        {                            
                            writer.Write("\"");
                            writer.Write(value);
                            writer.Write("\"");                         
                        }
                        else
                        {                            
                            writer.Write((int)value);                            
                        }

                        return;
                    }                    

                    if (valueType.IsArray || LinqMethodsSupport.IsCollection(valueType))
                    {
                        var arr = value as object[] ?? (value as IEnumerable<object>)?.ToArray();
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

                        return;
                    }

                    context.PreventDefault();

                    // if we have a number, write the value without quotation
                    if (_numericTypes.Contains(valueType) || _numericTypes.Contains(Nullable.GetUnderlyingType(valueType)))
                    {
                        writer.Write(value.ToInvariantString());
                    }
                    else
                    {
                        writer.Write("\"");
                        writer.Write(value);
                        writer.Write("\"");                        
                    }                    
                }
            }

            private static readonly HashSet<Type> _numericTypes = new HashSet<Type>
            {
                typeof(Byte),
                typeof(SByte),
                typeof(Int16),
                typeof(UInt16),
                typeof(Int32),
                typeof(UInt32),
                typeof(Int64),
                typeof(UInt64),
                typeof(double),
                typeof(decimal),
                typeof(System.Numerics.BigInteger),

            };
        }

        public class ConstSupport : JavascriptConversionExtension
        {
            private readonly DocumentConventions _conventions;

            public ConstSupport(DocumentConventions conventions)
            {
                _conventions = conventions;
            }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is ConstantExpression nodeAsConst))
                    return;

                var writer = context.GetWriter();
                using (writer.Operation(nodeAsConst))
                {
                    object nodeValue = nodeAsConst.Value;
                    Type nodeType = nodeAsConst.Type;

                    if (nodeValue == null)
                    {
                        context.PreventDefault();
                        writer.Write("null");
                        return;
                    }                    

                    // Type.IsEnum is unavailable in .netstandard1.3
                    if (nodeType.GetTypeInfo().IsEnum
                        && _conventions.SaveEnumsAsIntegers == false)
                    {
                        context.PreventDefault();
                        writer.Write("\"");
                        writer.Write(nodeValue);
                        writer.Write("\"");
                    }

                    if (nodeType.IsArray || LinqMethodsSupport.IsCollection(nodeType))
                    {
                        var arr = nodeValue as object[] ?? (nodeValue as IEnumerable<object>)?.ToArray();
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
            public static NullCoalescingSupport Instance = new NullCoalescingSupport();

            private NullCoalescingSupport()
            {
            }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (context.Node.NodeType != ExpressionType.Coalesce || !(context.Node is BinaryExpression binaryExpression))
                    return;

                context.PreventDefault();

                var test = Expression.NotEqual(
                        binaryExpression.Left,
                        Expression.Constant(null, binaryExpression.Left.Type)
                    );

                var condition = Expression.Condition(test, 
                    binaryExpression.Left, 
                    Expression.Convert(binaryExpression.Right, binaryExpression.Left.Type));

                var writer = context.GetWriter();
                writer.Write('(');
                context.Visitor.Visit(condition);
                writer.Write(')');
            }
        }

        public class ListInitSupport : JavascriptConversionExtension
        {
            public static readonly ListInitSupport Instance = new ListInitSupport();

            private ListInitSupport()
            {
            }

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
            public static readonly NewSupport Instance = new NewSupport();

            private NewSupport()
            {
            }

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
                                if (member.Name.StartsWith(TransparentIdentifier))
                                {
                                    name = name.Replace(TransparentIdentifier, DefaultAliasPrefix);
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
                            val.Value is int asInt)
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
            public static readonly NullComparisonSupport Instance = new NullComparisonSupport();

            private NullComparisonSupport()
            {
            }

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
            public static readonly NestedConditionalSupport Instance = new NestedConditionalSupport();

            private NestedConditionalSupport()
            {
            }

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
            public static readonly StringSupport Instance = new StringSupport();

            private StringSupport()
            {
            }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is MethodCallExpression mce) || 
                    mce.Method.DeclaringType != typeof(string))
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
                    case "Format":
                        newName = "format";
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
                                WriteArguments(context, mce.Arguments, writer, 1);
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
                        case "format":
                            context.Visitor.Visit(mce.Arguments[0]);
                            writer.Write(".format(");
                            if (mce.Arguments.Count == 2 && mce.Arguments[1] is NewArrayExpression nae)
                            {
                                WriteArguments(context, nae.Expressions, writer);
                            }
                            else
                            {
                                WriteArguments(context, mce.Arguments, writer, 1);
                            }
                            writer.Write(")");
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
                                else if (mce.Arguments[0] is MethodCallExpression mce2)
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
                                            throw new InvalidOperationException("Unable to understand how to convert " + value + " to RQL (" + value?.GetType() ?? "null" + ")");
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
                                WriteArguments(context, mce.Arguments, writer);
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
            public static readonly MetadataSupport Instance = new MetadataSupport();

            private MetadataSupport()
            {
            }

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
            private readonly DocumentConventions _conventions;

            public IdentityPropertySupport(DocumentConventions conventions)
            {
                _conventions = conventions;
            }

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                if (!(context.Node is MemberExpression member))
                    return;

                if (member.Expression is ParameterExpression parameter
                    && _conventions.GetIdentityProperty(member.Member.DeclaringType) == member.Member)
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

                var p = GetParameter(innerMember)?.Name;

                if (p != null && p.StartsWith(TransparentIdentifier)
                    && _conventions.GetIdentityProperty(member.Member.DeclaringType) == member.Member)
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
        }

        public static ParameterExpression GetParameter(MemberExpression expression)
        {
            return GetInnermostExpression(expression, out _) as ParameterExpression;
        }

        public static Expression GetInnermostExpression(MemberExpression expression, out string path)
        {
            path = string.Empty;
            while (expression.Expression is MemberExpression memberExpression)
            {
                expression = memberExpression;
                path = path == string.Empty 
                    ? expression.Member.Name 
                    : $"{expression.Member.Name}.{path}";
            }

            return expression.Expression;
        }

        private static void WriteArguments(JavascriptConversionContext context, IReadOnlyList<Expression> arguments, JavascriptWriter writer, int start = 0)
        {
            for (var i = start; i < arguments.Count; i++)
            {
                if (i != start)
                {
                    writer.Write(", ");
                }

                context.Visitor.Visit(arguments[i]);
            }
        }
    }
}
