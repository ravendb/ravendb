using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Analyzers.Shared
{
    public static class SyntaxHelpers
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
        /// Like <see cref="EnumerateInvocationChain(ExpressionSyntax?)"/>, but when the chain bottoms
        /// out at a local variable reference it follows the variable to its single initializer and
        /// keeps walking. This lets chain-based analyzers see through a fluent query that was split
        /// across statements (<c>var q = session.Query&lt;T&gt;().Take(10); q.ToList();</c>). A small
        /// hop budget guards against pathological self-referential declarations.
        /// </summary>
        internal static IEnumerable<InvocationExpressionSyntax> EnumerateInvocationChain(ExpressionSyntax? expression, SemanticModel model)
        {
            int variableHops = 0;
            while (expression != null)
            {
                switch (expression)
                {
                    case InvocationExpressionSyntax invocation:
                        yield return invocation;
                        if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
                        {
                            expression = memberAccess.Expression;
                            continue;
                        }
                        yield break;

                    case ParenthesizedExpressionSyntax paren:
                        expression = paren.Expression;
                        continue;

                    default:
                        if (variableHops++ >= 32)
                            yield break;
                        expression = TryResolveLocalInitializer(expression, model);
                        continue;
                }
            }
        }

        /// <summary>
        /// When <paramref name="expression"/> is a simple reference to a local variable that has a
        /// single initializer, returns that initializer expression; otherwise null. Used to follow a
        /// query/projection chain across a <c>var x = …;</c> assignment.
        /// </summary>
        internal static ExpressionSyntax? TryResolveLocalInitializer(ExpressionSyntax? expression, SemanticModel model)
        {
            if (expression is not IdentifierNameSyntax id)
                return null;

            if (model.GetSymbolInfo(id).Symbol is not ILocalSymbol local)
                return null;

            if (local.DeclaringSyntaxReferences.IsDefaultOrEmpty)
                return null;

            return local.DeclaringSyntaxReferences[0].GetSyntax() is VariableDeclaratorSyntax declarator
                ? declarator.Initializer?.Value
                : null;
        }

        /// <summary>
        /// Returns the simple method name from an invocation, handling both
        /// plain member access and generic method calls.
        /// </summary>
        public static string? GetMethodName(InvocationExpressionSyntax invocation)
        {
            return invocation.Expression switch
            {
                MemberAccessExpressionSyntax memberAccess => memberAccess.Name.Identifier.Text,
                GenericNameSyntax genericName => genericName.Identifier.Text,
                _ => null,
            };
        }

        /// <summary>
        /// Returns true when <paramref name="type"/>, or any base type up the chain, satisfies
        /// <paramref name="predicate"/>. The single base-type walk shared by every "is this a known
        /// Raven base class" check so they cannot drift apart.
        /// </summary>
        internal static bool AnyBaseTypeOrSelf(ITypeSymbol? type, Func<ITypeSymbol, bool> predicate)
        {
            for (ITypeSymbol? current = type; current != null; current = current.BaseType)
            {
                if (predicate(current))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Returns true when <paramref name="type"/> is named <paramref name="name"/> or implements
        /// an interface of that name. Matched by simple name only — avoids FQN coupling with
        /// Raven.Client versioning (a deliberate design choice so the analyzers stay in sync with
        /// client refactoring; see readme.md).
        /// </summary>
        internal static bool IsTypeOrImplements(ITypeSymbol? type, string name)
        {
            if (type == null)
                return false;

            if (type.Name == name)
                return true;

            foreach (INamedTypeSymbol iface in type.AllInterfaces)
            {
                if (iface.Name == name)
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Returns true when <paramref name="type"/> is, or implements, IRavenQueryable&lt;T&gt;.
        /// </summary>
        public static bool IsRavenQueryable(ITypeSymbol? type) =>
            IsTypeOrImplements(type, KnownTypes.IRavenQueryableName);

        /// <summary>
        /// Returns true when <paramref name="type"/> is, or implements, IDocumentSession or
        /// IAsyncDocumentSession.
        /// </summary>
        public static bool IsSessionType(ITypeSymbol? type) =>
            IsTypeOrImplements(type, KnownTypes.IDocumentSessionName)
            || IsTypeOrImplements(type, KnownTypes.IAsyncDocumentSessionName);

        /// <summary>
        /// Returns true when <paramref name="type"/> is, or implements, IDocumentStore.
        /// </summary>
        public static bool IsDocumentStore(ITypeSymbol? type) =>
            IsTypeOrImplements(type, KnownTypes.IDocumentStoreName);

        /// <summary>
        /// Returns true when <paramref name="type"/>, or any base type, is a subscription worker
        /// (<c>SubscriptionWorker</c> / <c>AbstractSubscriptionWorker</c>).
        /// </summary>
        public static bool IsSubscriptionWorkerType(ITypeSymbol? type) =>
            AnyBaseTypeOrSelf(type, t =>
                t.Name == KnownTypes.SubscriptionWorkerTypeName
                || t.Name == KnownTypes.AbstractSubscriptionWorkerTypeName);

        /// <summary>
        /// True when <paramref name="symbol"/> is declared in the compilation's own source (a
        /// user-defined member) rather than coming from a referenced assembly (BCL, Raven.Client).
        /// Used to tell a genuine framework method (e.g. <c>Enumerable.ToList</c>, Raven's async
        /// query extensions) apart from a same-named user-defined extension.
        /// </summary>
        public static bool IsUserDefinedInSource(ISymbol? symbol)
        {
            if (symbol == null)
                return false;

            foreach (Location location in symbol.Locations)
            {
                if (location.IsInSource)
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
        internal static bool IsMultiMapIndexCreationTask(INamedTypeSymbol? symbol) =>
            AnyBaseTypeOrSelf(symbol?.BaseType, t => IsMultiMapBase(t as INamedTypeSymbol));

        /// <summary>
        /// Returns true when the class (or any of its base classes up the chain) is a known
        /// index creation task base: <c>AbstractIndexCreationTask&lt;T&gt;</c> /
        /// <c>AbstractGenericIndexCreationTask</c> (document indexes) and the multi-map bases
        /// (<c>AbstractMultiMapIndexCreationTask</c> and its time-series / counters variants).
        /// Regular (non-multi-map) time-series and counters index bases are intentionally not
        /// matched yet — their map conventions differ and analyzer support for them is a follow-up.
        /// </summary>
        internal static bool IsIndexCreationTask(INamedTypeSymbol? symbol) =>
            AnyBaseTypeOrSelf(symbol?.BaseType, t =>
                t.Name is KnownTypes.AbstractIndexCreationTaskGenericName
                       or KnownTypes.AbstractGenericIndexCreationTaskName
                       or KnownTypes.AbstractMultiMapIndexCreationTaskName
                       or KnownTypes.AbstractMultiMapTimeSeriesIndexCreationTaskName
                       or KnownTypes.AbstractMultiMapCountersIndexCreationTaskName);

        /// <summary>
        /// True when <paramref name="type"/> is <em>itself</em> one of the framework index base
        /// classes (<c>AbstractIndexCreationTask&lt;T&gt;</c>, <c>AbstractGenericIndexCreationTask</c>,
        /// or a multi-map base). Marks the boundary at which a walk over user-defined index classes
        /// up the base chain should stop — the framework base does not assign Map/AddMap in user source.
        /// </summary>
        internal static bool IsKnownIndexBaseType(ITypeSymbol? type) =>
            type?.Name is KnownTypes.AbstractIndexCreationTaskGenericName
                       or KnownTypes.AbstractGenericIndexCreationTaskName
                       or KnownTypes.AbstractMultiMapIndexCreationTaskName
                       or KnownTypes.AbstractMultiMapTimeSeriesIndexCreationTaskName
                       or KnownTypes.AbstractMultiMapCountersIndexCreationTaskName;

        /// <summary>
        /// Returns true when <paramref name="containingType"/> (or any base type) is
        /// a known index-base class that declares Map/Reduce.
        /// </summary>
        internal static bool IsDefinedOnIndexBase(INamedTypeSymbol? containingType) =>
            AnyBaseTypeOrSelf(containingType, t =>
                t.Name is KnownTypes.AbstractIndexCreationTaskGenericName
                       or KnownTypes.AbstractGenericIndexCreationTaskName);

        /// <summary>
        /// Returns true when any base class of <paramref name="symbol"/> starts with
        /// <see cref="KnownTypes.AbstractJavaScriptIndexCreationTaskName"/>.
        /// JavaScript-based indexes cannot be statically analyzed.
        /// </summary>
        internal static bool IsJavaScriptIndex(INamedTypeSymbol? symbol) =>
            AnyBaseTypeOrSelf(symbol?.BaseType, t =>
                t.Name.StartsWith(KnownTypes.AbstractJavaScriptIndexCreationTaskName, StringComparison.Ordinal));

        /// <summary>
        /// Unwraps a simple or parenthesized lambda to its expression body.
        /// Returns null when the lambda uses a block body or when <paramref name="expr"/>
        /// is not a lambda at all.
        /// </summary>
        internal static ExpressionSyntax? TryGetLambdaBody(ExpressionSyntax? expr) =>
            expr switch
            {
                SimpleLambdaExpressionSyntax { Body: ExpressionSyntax e } => e,
                ParenthesizedLambdaExpressionSyntax { Body: ExpressionSyntax e } => e,
                _ => null,
            };

        /// <summary>
        /// If <paramref name="node"/> is an index <c>Map</c> (or, when <paramref name="includeReduce"/>
        /// is set, <c>Reduce</c>) assignment on the index base, or an <c>AddMap</c>/<c>AddMapForAll</c>
        /// invocation on a multi-map base, returns the lambda's body; otherwise null. Shared by the
        /// fan-out (Map/AddMap only) and unsupported-method (Map+Reduce/AddMap) index analyzers so the
        /// recognition of what counts as a map/reduce lambda cannot drift between them.
        /// </summary>
        internal static SyntaxNode? TryGetIndexMapLambdaBody(SyntaxNode node, SemanticModel model, bool includeReduce)
        {
            // Map = lambda  /  this.Map = lambda  /  base.Reduce = lambda  etc.
            if (node is AssignmentExpressionSyntax assignment)
            {
                SimpleNameSyntax? nameNode = TryGetSimpleMemberName(assignment.Left);
                if (nameNode == null)
                    return null;

                string name = nameNode.Identifier.Text;
                bool matches = name == KnownTypes.MapFieldName
                               || (includeReduce && name == KnownTypes.ReduceFieldName);
                if (!matches)
                    return null;

                ISymbol? sym = model.GetSymbolInfo(nameNode).Symbol;
                if (sym is not (IFieldSymbol or IPropertySymbol))
                    return null;

                if (!IsDefinedOnIndexBase(sym.ContainingType))
                    return null;

                return TryGetLambdaBody(assignment.Right);
            }

            // AddMap<T>(...) or AddMapForAll<T>(...)
            if (node is InvocationExpressionSyntax invocation)
            {
                string? methodName = GetMethodName(invocation);
                if (methodName != KnownTypes.AddMapMethodName && methodName != KnownTypes.AddMapForAllMethodName)
                    return null;

                ISymbol? sym = model.GetSymbolInfo(invocation).Symbol;
                if (sym is not IMethodSymbol method || !IsMultiMapBase(method.ContainingType))
                    return null;

                SeparatedSyntaxList<ArgumentSyntax> args = invocation.ArgumentList.Arguments;
                if (args.Count == 0)
                    return null;

                return TryGetLambdaBody(args[args.Count - 1].Expression);
            }

            return null;
        }

        /// <summary>
        /// Returns the effective body of a method-like member as a single <see cref="SyntaxNode"/>:
        /// the <see cref="BaseMethodDeclarationSyntax.Body"/> block when present, otherwise the
        /// expression from the <c>=&gt;</c> arrow body, or <c>null</c> when the member has neither
        /// (e.g. an abstract method or a declaration-only extern).
        /// Covers constructors, ordinary methods, and any other <see cref="BaseMethodDeclarationSyntax"/>.
        /// </summary>
        internal static SyntaxNode? GetBodyNode(this BaseMethodDeclarationSyntax member) =>
            (SyntaxNode?)member.Body ?? member.ExpressionBody?.Expression;

        /// <summary>
        /// Extracts the <see cref="SimpleNameSyntax"/> node that names a member reference, handling both
        /// the bare form (<c>Map = …</c> / <c>Stores[…]</c>) and the qualified forms
        /// (<c>this.Map = …</c> / <c>base.Stores[…]</c>).
        /// Returns <c>null</c> for any other shape.
        /// </summary>
        internal static SimpleNameSyntax? TryGetSimpleMemberName(ExpressionSyntax expression)
        {
            if (expression is IdentifierNameSyntax id)
                return id;

            if (expression is MemberAccessExpressionSyntax ma
                && ma.Expression is ThisExpressionSyntax or BaseExpressionSyntax)
                return ma.Name;

            return null;
        }

        /// <summary>
        /// Returns the single parameter name of a simple or one-parameter parenthesized lambda,
        /// or null for any other shape.
        /// </summary>
        internal static string? GetLambdaParameterName(ExpressionSyntax expr) =>
            expr switch
            {
                SimpleLambdaExpressionSyntax simple => simple.Parameter.Identifier.ValueText,
                ParenthesizedLambdaExpressionSyntax paren when paren.ParameterList.Parameters.Count == 1
                    => paren.ParameterList.Parameters[0].Identifier.ValueText,
                _ => null,
            };

        /// <summary>
        /// True when <paramref name="body"/> contains a dynamic-field-creating call
        /// (<c>CreateField</c>, <c>CreateSpatialField</c>, <c>AsJson</c>). Such an index projects
        /// field names only known at runtime, so the static field/stored-field extractors must bail.
        /// </summary>
        internal static bool ContainsDynamicFieldCalls(SyntaxNode body)
        {
            foreach (InvocationExpressionSyntax inv in body.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
            {
                string? name = GetMethodName(inv);
                if (name == null && inv.Expression is IdentifierNameSyntax id)
                    name = id.Identifier.Text;

                if (name == KnownTypes.CreateFieldMethodName
                    || name == KnownTypes.CreateSpatialFieldMethodName
                    || name == KnownTypes.AsJsonMethodName)
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Returns the location of the method-name token of an invocation (the member name for
        /// <c>x.Method()</c>, the identifier for <c>Method()</c>), falling back to the whole
        /// invocation. Used to anchor diagnostics on the called method rather than the full expression.
        /// </summary>
        internal static Location GetInvocationNameLocation(InvocationExpressionSyntax invocation) =>
            invocation.Expression switch
            {
                MemberAccessExpressionSyntax memberAccess => memberAccess.Name.GetLocation(),
                IdentifierNameSyntax identifier => identifier.GetLocation(),
                _ => invocation.GetLocation(),
            };
    }
}
