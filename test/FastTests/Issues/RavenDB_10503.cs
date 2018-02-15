using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Raven.Client.Documents.Session;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_10503 : NoDisposalNeeded
    {
        private static readonly HashSet<string> MethodsToSkip = new HashSet<string>
        {
            nameof(Enumerable.ToList),          // materializer
            nameof(Enumerable.ToArray),         // materializer
            nameof(Enumerable.ToDictionary),    // materializer
            nameof(Enumerable.ToHashSet),       // materializer
            nameof(Enumerable.ToLookup),        // materializer
            nameof(Enumerable.Equals),          // not needed
            nameof(Enumerable.Range),           // not an extension
            nameof(Enumerable.Repeat),          // not an extension
            nameof(Enumerable.SkipLast),        // not available in netstandard2.0
            nameof(Enumerable.TakeLast),        // not available in netstandard2.0
            nameof(Enumerable.ThenBy),          // IOrderedEnumerable - not needed
            nameof(Enumerable.ThenByDescending),// IOrderedEnumerable - not needed
            nameof(Enumerable.OfType),          // implemented
            nameof(Enumerable.Cast)             // how to overwrite it?
        };

        [Fact]
        public void InterfaceIDocumentQueryAndIRawDocumentQueryShouldOverrideAllEnumerableExtensions()
        {
            var missingDocumentQueryMethods = new List<string>();
            var missingRawDocumentQueryMethods = new List<string>();

            var documentQueryTypeBase = typeof(IDocumentQuery<>).GetGenericTypeDefinition();
            var rawDocumentQueryTypeBase = typeof(IRawDocumentQuery<>).GetGenericTypeDefinition();

            foreach (var method in typeof(Enumerable).GetMethods())
            {
                if (MethodsToSkip.Contains(method.Name))
                    continue;

                var allParameters = method.GetParameters();

                if (allParameters.Length == 0)
                    continue;

                var methodName = method.Name;

                var documentQueryMethod = FindMethod(documentQueryTypeBase, methodName, allParameters.Select(x => x.ParameterType).ToArray());
                if (documentQueryMethod == null)
                    missingDocumentQueryMethods.Add(GetSignature(method));

                var rawDocumentQueryMethod = FindMethod(rawDocumentQueryTypeBase, methodName, allParameters.Select(x => x.ParameterType).ToArray());
                if (rawDocumentQueryMethod == null)
                    missingRawDocumentQueryMethods.Add(GetSignature(method));
            }

            if (missingDocumentQueryMethods.Count > 0 || missingRawDocumentQueryMethods.Count > 0)
            {
                var sb = new StringBuilder();

                sb.AppendLine("=======================");
                sb.AppendLine(nameof(IDocumentQuery<object>));
                foreach (var method in missingDocumentQueryMethods)
                    sb.AppendLine(method);

                sb.AppendLine("=======================");
                sb.AppendLine(nameof(IRawDocumentQuery<object>));
                foreach (var method in missingRawDocumentQueryMethods)
                    sb.AppendLine(method);

                throw new InvalidOperationException(sb.ToString());
            }
        }

        /// <summary>
        /// DO NOT DELETE THIS
        /// </summary>
        /// <returns></returns>
        private static string GenerateDocumentQueryExtensions()
        {
            var sb = new StringBuilder();

            sb.AppendLine("namespace System.Linq");
            sb.AppendLine("{");

            sb.AppendLine("public static class DocumentQueryExtensions");
            sb.AppendLine("{");

            foreach (var method in typeof(Enumerable).GetMethods())
            {
                if (MethodsToSkip.Contains(method.Name))
                    continue;

                var allParameters = method.GetParameters();

                if (allParameters.Length == 0)
                    continue;

                var firstParameter = allParameters.First().ParameterType;
                if (firstParameter.IsGenericType)
                    firstParameter = firstParameter.GetGenericTypeDefinition();

                if (firstParameter != typeof(IEnumerable<>) && firstParameter != typeof(IEnumerable))
                    continue;

                WriteExtensionMethod(sb, method);
            }

            sb.AppendLine("}");
            sb.AppendLine("}");

            return sb.ToString();
        }

        private static void WriteExtensionMethod(StringBuilder sb, MethodInfo method)
        {
            sb.AppendLine("[Obsolete(\"This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.\")]");
            sb.AppendLine(GetSignature(method).Replace("this IEnumerable", "this IDocumentQuery"));
            sb.AppendLine("{");
            sb.AppendLine($"return Enumerable.{GetSignature(method, callable: true)};");
            sb.AppendLine("}");
            sb.AppendLine();

            sb.AppendLine("[Obsolete(\"This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.\")]");
            sb.AppendLine(GetSignature(method).Replace("this IEnumerable", "this IRawDocumentQuery"));
            sb.AppendLine("{");
            sb.AppendLine($"return Enumerable.{GetSignature(method, callable: true)};");
            sb.AppendLine("}");
            sb.AppendLine();
        }

        private static string GetSignature(MethodInfo method, bool callable = false)
        {
            var firstParam = true;
            var sigBuilder = new StringBuilder();
            if (callable == false)
            {
                if (method.IsPublic)
                    sigBuilder.Append("public ");
                else if (method.IsPrivate)
                    sigBuilder.Append("private ");
                else if (method.IsAssembly)
                    sigBuilder.Append("internal ");
                if (method.IsFamily)
                    sigBuilder.Append("protected ");
                if (method.IsStatic)
                    sigBuilder.Append("static ");
                sigBuilder.Append(TypeName(method.ReturnType));
                sigBuilder.Append(' ');
            }
            sigBuilder.Append(method.Name);

            // Add method generics
            if (method.IsGenericMethod)
            {
                sigBuilder.Append("<");
                foreach (var g in method.GetGenericArguments())
                {
                    if (firstParam)
                        firstParam = false;
                    else
                        sigBuilder.Append(", ");
                    sigBuilder.Append(TypeName(g));
                }
                sigBuilder.Append(">");
            }
            sigBuilder.Append("(");
            firstParam = true;
            var secondParam = false;
            foreach (var param in method.GetParameters())
            {
                if (firstParam)
                {
                    firstParam = false;
                    if (method.IsDefined(typeof(System.Runtime.CompilerServices.ExtensionAttribute), false))
                    {
                        if (callable)
                        {
                            sigBuilder.Append(param.Name);
                            //secondParam = true;
                            continue;
                        }
                        sigBuilder.Append("this ");
                    }
                }
                else if (secondParam == true)
                    secondParam = false;
                else
                    sigBuilder.Append(", ");
                if (param.ParameterType.IsByRef)
                    sigBuilder.Append("ref ");
                else if (param.IsOut)
                    sigBuilder.Append("out ");
                if (!callable)
                {
                    sigBuilder.Append(TypeName(param.ParameterType));
                    sigBuilder.Append(' ');
                }
                sigBuilder.Append(param.Name);
            }
            sigBuilder.Append(")");
            return sigBuilder.ToString();
        }

        private static string TypeName(Type type)
        {
            var nullableType = Nullable.GetUnderlyingType(type);
            if (nullableType != null)
                return nullableType.Name + "?";

            if (!(type.IsGenericType && type.Name.Contains('`')))
                switch (type.Name)
                {
                    case "String":
                        return "string";
                    case "Int32":
                        return "int";
                    case "Decimal":
                        return "decimal";
                    case "Object":
                        return "object";
                    case "Void":
                        return "void";
                    default:
                        {
                            return string.IsNullOrWhiteSpace(type.FullName) ? type.Name : type.FullName;
                        }
                }

            var sb = new StringBuilder(type.Name.Substring(0,
            type.Name.IndexOf('`'))
            );
            sb.Append('<');
            var first = true;
            foreach (var t in type.GetGenericArguments())
            {
                if (!first)
                    sb.Append(',');
                sb.Append(TypeName(t));
                first = false;
            }
            sb.Append('>');
            return sb.ToString();
        }

        private static MethodInfo FindMethod(Type type, string methodName, Type[] methodParameters)
        {
            foreach (var methodInfo in typeof(DocumentQueryExtensions).GetMethods())
            {
                if (methodInfo.Name != methodName)
                    continue;

                var parameters = methodInfo.GetParameters();
                if (parameters.Length != methodParameters.Length)
                    continue;

                var firstParameter = parameters[0].ParameterType;
                if (firstParameter.IsGenericType == false)
                    continue;

                firstParameter = firstParameter.GetGenericTypeDefinition();
                if (firstParameter != type)
                    continue;

                for (var index = 1; index < parameters.Length; index++)
                {
                    var parameter = parameters[index].ParameterType;
                    var methodParameter = methodParameters[index];

                    if (TypesAreEqual(parameter, methodParameter) == false)
                        break;
                }

                return methodInfo;
            }

            return null;
        }

        private static bool TypesAreEqual(Type type1, Type type2)
        {
            if (type1.IsGenericType != type2.IsGenericType)
                return false;

            if (type1.IsGenericType)
            {
                if (type1.GetGenericTypeDefinition() != type2.GetGenericTypeDefinition())
                    return false;

                var type1Arguments = type1.GenericTypeArguments;
                var type2Arguments = type2.GenericTypeArguments;

                for (int i = 0; i < type1Arguments.Length; i++)
                {
                    var type1Argument = type1Arguments[i];
                    var type2Argument = type2Arguments[i];

                    if (TypesAreEqual(type1Argument, type2Argument) == false)
                        return false;
                }

                return true;
            }

            if (type1.IsGenericParameter && type2.IsGenericParameter)
                return true;

            if (type1 != type2)
                return false;

            return true;
        }
    }
}
