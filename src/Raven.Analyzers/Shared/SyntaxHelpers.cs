using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Analyzers.Shared
{
    internal static class SyntaxHelpers
    {
        /// <summary>
        /// Enumerates the invocation chain starting from <paramref name="expression"/>,
        /// walking from outer to inner by following the receiver of each MemberAccessExpression.
        /// </summary>
        internal static IEnumerable<InvocationExpressionSyntax> EnumerateInvocationChain(ExpressionSyntax? expression)
        {
            while (expression is InvocationExpressionSyntax invocation)
            {
                yield return invocation;

                if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
                    expression = memberAccess.Expression;
                else
                    yield break;
            }
        }

        /// <summary>
        /// Returns the simple method name from an invocation, handling both
        /// plain member access and generic method calls.
        /// </summary>
        internal static string? GetMethodName(InvocationExpressionSyntax invocation)
        {
            return invocation.Expression switch
            {
                MemberAccessExpressionSyntax memberAccess => memberAccess.Name.Identifier.Text,
                GenericNameSyntax genericName => genericName.Identifier.Text,
                _ => null,
            };
        }

        /// <summary>
        /// Returns true when <paramref name="type"/> is, or implements, IRavenQueryable&lt;T&gt;.
        /// Matched by name only — avoids FQN coupling with Raven.Client versioning.
        /// </summary>
        internal static bool IsRavenQueryable(ITypeSymbol? type)
        {
            if (type == null)
                return false;

            if (type.Name == KnownTypes.IRavenQueryableName)
                return true;

            foreach (INamedTypeSymbol iface in type.AllInterfaces)
            {
                if (iface.Name == KnownTypes.IRavenQueryableName)
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Returns true when <paramref name="type"/> is itself one of the multi-map base classes
        /// (checked by name, not by walking ancestors).
        /// Used to verify that an <c>AddMap</c> call resolves to the right containing type.
        /// </summary>
        internal static bool IsMultiMapBase(INamedTypeSymbol? type) =>
            type?.Name is KnownTypes.AbstractMultiMapIndexCreationTaskName
                       or KnownTypes.AbstractMultiMapTimeSeriesIndexCreationTaskName
                       or KnownTypes.AbstractMultiMapCountersIndexCreationTaskName;

        /// <summary>
        /// Returns true when any base class of <paramref name="symbol"/> is one of the multi-map variants:
        /// AbstractMultiMapIndexCreationTask, AbstractMultiMapTimeSeriesIndexCreationTask,
        /// or AbstractMultiMapCountersIndexCreationTask.
        /// </summary>
        internal static bool IsMultiMapIndexCreationTask(INamedTypeSymbol? symbol)
        {
            INamedTypeSymbol? current = symbol?.BaseType;
            while (current != null)
            {
                if (IsMultiMapBase(current))
                    return true;

                current = current.BaseType;
            }

            return false;
        }

        /// <summary>
        /// Returns true when the class (or any of its base classes up the chain) is a known
        /// index creation task base. Covers document, time-series, and counters variants
        /// (both regular and multi-map).
        /// </summary>
        internal static bool IsIndexCreationTask(INamedTypeSymbol? symbol)
        {
            INamedTypeSymbol? current = symbol?.BaseType;
            while (current != null)
            {
                string name = current.Name;
                if (name is KnownTypes.AbstractIndexCreationTaskGenericName
                         or KnownTypes.AbstractGenericIndexCreationTaskName
                         or KnownTypes.AbstractMultiMapIndexCreationTaskName
                         or KnownTypes.AbstractMultiMapTimeSeriesIndexCreationTaskName
                         or KnownTypes.AbstractMultiMapCountersIndexCreationTaskName)
                {
                    return true;
                }

                current = current.BaseType;
            }

            return false;
        }

        /// <summary>
        /// Returns true when <paramref name="containingType"/> (or any base type) is
        /// a known index-base class that declares Map/Reduce.
        /// </summary>
        internal static bool IsDefinedOnIndexBase(INamedTypeSymbol? containingType)
        {
            INamedTypeSymbol? current = containingType;
            while (current != null)
            {
                if (current.Name is KnownTypes.AbstractIndexCreationTaskGenericName
                                 or KnownTypes.AbstractGenericIndexCreationTaskName)
                {
                    return true;
                }

                current = current.BaseType;
            }

            return false;
        }

        /// <summary>
        /// Returns true when any base class of <paramref name="symbol"/> starts with
        /// <see cref="KnownTypes.AbstractJavaScriptIndexCreationTaskName"/>.
        /// JavaScript-based indexes cannot be statically analyzed.
        /// </summary>
        internal static bool IsJavaScriptIndex(INamedTypeSymbol? symbol)
        {
            INamedTypeSymbol? current = symbol?.BaseType;
            while (current != null)
            {
                if (current.Name.StartsWith(KnownTypes.AbstractJavaScriptIndexCreationTaskName,
                    System.StringComparison.Ordinal))
                    return true;
                current = current.BaseType;
            }
            return false;
        }

        /// <summary>
        /// Unwraps a simple or parenthesized lambda to its expression body.
        /// Returns null when the lambda uses a block body or when <paramref name="expr"/>
        /// is not a lambda at all.
        /// </summary>
        internal static ExpressionSyntax? TryGetLambdaBody(ExpressionSyntax? expr) =>
            expr switch
            {
                SimpleLambdaExpressionSyntax simple when simple.Body is ExpressionSyntax e => e,
                ParenthesizedLambdaExpressionSyntax paren when paren.Body is ExpressionSyntax e => e,
                _ => null,
            };
    }
}
