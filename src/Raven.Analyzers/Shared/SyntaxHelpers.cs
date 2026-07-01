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
        /// Hop budget for the chain walkers that follow a query/projection through local-variable
        /// initializers. Guards against a pathological self-referential declaration. Shared so every
        /// spine walk uses the same bound rather than each picking its own magic number.
        /// </summary>
        internal const int MaxInvocationChainHops = 32;


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
                        if (variableHops++ >= MaxInvocationChainHops)
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
        /// Walks a fluent chain such as <c>session.Query&lt;T&gt;().Where(...).OrderBy(...)</c> down through
        /// invocation receivers and parentheses to the root expression (typically the session/store
        /// reference). Shared by the lazy-batching analyzer (to resolve the receiver's symbol) and its
        /// code fix (to extract the receiver node), so the two cannot disagree on what the chain root is.
        /// </summary>
        public static ExpressionSyntax WalkInvocationChainToRoot(ExpressionSyntax expression)
        {
            ExpressionSyntax current = expression;
            while (true)
            {
                switch (current)
                {
                    case InvocationExpressionSyntax inv when inv.Expression is MemberAccessExpressionSyntax ma:
                        current = ma.Expression;
                        continue;
                    case ParenthesizedExpressionSyntax paren:
                        current = paren.Expression;
                        continue;
                    default:
                        return current;
                }
            }
        }

        /// <summary>
        /// Returns <paramref name="symbol"/> when it denotes a stable instance — a local, parameter, or
        /// field — that two calls can be proven to share, otherwise null. A property getter or method
        /// call (e.g. <c>GetSession()</c> or a <c>Session</c> property) may return a fresh instance per
        /// invocation, so such receivers must not be grouped as "the same session". Shared by the
        /// lazy-batching analyzer (grouping) and its code fix (same-session bail) so the predicate that
        /// keeps them in lockstep lives in exactly one place.
        /// </summary>
        public static ISymbol? AsStableSessionInstance(ISymbol? symbol) =>
            symbol is ILocalSymbol or IParameterSymbol or IFieldSymbol ? symbol : null;

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
        /// an interface of that name, AND that matching type/interface is declared in the
        /// <c>Raven.Client</c> namespace. Matching by simple name (not full assembly-qualified name)
        /// keeps the analyzers decoupled from Raven.Client versioning, while the namespace gate stops
        /// an unrelated user type that happens to be named <c>IDocumentSession</c>,
        /// <c>IRavenQueryable</c>, etc. from being treated as the RavenDB type.
        /// </summary>
        internal static bool IsTypeOrImplements(ITypeSymbol? type, string name)
        {
            if (type == null)
                return false;

            if (type.Name == name && IsInRavenClientNamespace(type))
                return true;

            foreach (INamedTypeSymbol iface in type.AllInterfaces)
            {
                if (iface.Name == name && IsInRavenClientNamespace(iface))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// True when <paramref name="symbol"/> is declared in the <c>Raven.Client</c> namespace or a
        /// nested namespace under it (e.g. <c>Raven.Client.Documents.Session</c>).
        /// </summary>
        internal static bool IsInRavenClientNamespace(ISymbol symbol)
        {
            INamespaceSymbol? ns = symbol.ContainingNamespace;
            if (ns == null || ns.IsGlobalNamespace)
                return false;

            string full = ns.ToDisplayString();
            return full == KnownTypes.RavenClientNamespace
                   || full.StartsWith(KnownTypes.RavenClientNamespace + ".", StringComparison.Ordinal);
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
        /// Returns true when <paramref name="type"/> is, or implements, the Raven.Client
        /// <c>SessionOptions</c> type.
        /// </summary>
        public static bool IsSessionOptionsType(ITypeSymbol? type) =>
            IsTypeOrImplements(type, KnownTypes.SessionOptionsTypeName);

        /// <summary>
        /// Returns true when <paramref name="type"/>, or any base type, is a subscription worker
        /// (<c>SubscriptionWorker</c> / <c>AbstractSubscriptionWorker</c>) declared in the
        /// <c>Raven.Client</c> namespace. The namespace gate matches every other Raven-type check
        /// (see <see cref="IsTypeOrImplements"/>) so an unrelated user type that merely happens to be
        /// named <c>SubscriptionWorker</c> is not treated as the RavenDB type (a RVN011 false positive).
        /// </summary>
        public static bool IsSubscriptionWorkerType(ITypeSymbol? type) =>
            AnyBaseTypeOrSelf(type, t =>
                (t.Name == KnownTypes.SubscriptionWorkerTypeName
                 || t.Name == KnownTypes.AbstractSubscriptionWorkerTypeName)
                && IsInRavenClientNamespace(t));

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
        /// Classifies <paramref name="node"/> with respect to index map/reduce definitions:
        /// it is not a map node, it is a map node whose lambda body cannot be extracted
        /// (block body, or an <c>AddMap</c> with no arguments), or its expression body was extracted.
        /// </summary>
        internal enum IndexMapNodeKind
        {
            /// <summary>Not a <c>Map</c>/<c>Reduce</c> assignment or <c>AddMap</c>/<c>AddMapForAll</c> on the right base.</summary>
            NotAMapNode,
            /// <summary>A map definition node, but its lambda body cannot be extracted (block body, or no arguments).</summary>
            Unanalyzable,
            /// <summary>A map definition node whose expression lambda body was extracted.</summary>
            LambdaBody,
        }

        /// <summary>
        /// Core matcher for index map/reduce definition sites. Returns whether <paramref name="node"/>
        /// is an index <c>Map</c> (or, when <paramref name="includeReduce"/> is set, <c>Reduce</c>)
        /// assignment on the index base, or an <c>AddMap</c>/<c>AddMapForAll</c> invocation on a
        /// multi-map base, and — for the <see cref="IndexMapNodeKind.LambdaBody"/> case — outputs the
        /// lambda's expression body. The three-state result lets a caller that must distinguish
        /// "not a map" from "a map we cannot read" (the field extractor, which bails on the latter)
        /// share this recognition with callers that only need the body or null (the fan-out and
        /// unsupported-method analyzers, via <see cref="TryGetIndexMapLambdaBody"/>), so the notion of
        /// what counts as a map/reduce lambda cannot drift between them.
        /// </summary>
        internal static IndexMapNodeKind ClassifyIndexMapNode(SyntaxNode node, SemanticModel model, bool includeReduce, out ExpressionSyntax? lambdaBody)
        {
            lambdaBody = null;

            // Map = lambda  /  this.Map = lambda  /  base.Reduce = lambda  etc.
            if (node is AssignmentExpressionSyntax assignment)
            {
                SimpleNameSyntax? nameNode = TryGetSimpleMemberName(assignment.Left);
                if (nameNode == null)
                    return IndexMapNodeKind.NotAMapNode;

                string name = nameNode.Identifier.Text;
                bool matches = name == KnownTypes.MapFieldName
                               || (includeReduce && name == KnownTypes.ReduceFieldName);
                if (!matches)
                    return IndexMapNodeKind.NotAMapNode;

                ISymbol? sym = model.GetSymbolInfo(nameNode).Symbol;
                if (sym is not (IFieldSymbol or IPropertySymbol))
                    return IndexMapNodeKind.NotAMapNode;

                if (!IsDefinedOnIndexBase(sym.ContainingType))
                    return IndexMapNodeKind.NotAMapNode;

                lambdaBody = TryGetLambdaBody(assignment.Right);
                return lambdaBody != null ? IndexMapNodeKind.LambdaBody : IndexMapNodeKind.Unanalyzable;
            }

            // AddMap<T>(...) or AddMapForAll<T>(...)
            if (node is InvocationExpressionSyntax invocation)
            {
                string? methodName = GetMethodName(invocation);
                if (methodName != KnownTypes.AddMapMethodName && methodName != KnownTypes.AddMapForAllMethodName)
                    return IndexMapNodeKind.NotAMapNode;

                ISymbol? sym = model.GetSymbolInfo(invocation).Symbol;
                if (sym is not IMethodSymbol method || !IsMultiMapBase(method.ContainingType))
                    return IndexMapNodeKind.NotAMapNode;

                SeparatedSyntaxList<ArgumentSyntax> args = invocation.ArgumentList.Arguments;
                if (args.Count == 0)
                    return IndexMapNodeKind.Unanalyzable;

                lambdaBody = TryGetLambdaBody(args[args.Count - 1].Expression);
                return lambdaBody != null ? IndexMapNodeKind.LambdaBody : IndexMapNodeKind.Unanalyzable;
            }

            return IndexMapNodeKind.NotAMapNode;
        }

        /// <summary>
        /// Convenience over <see cref="ClassifyIndexMapNode"/> for callers that only need the lambda
        /// body or null: returns the extracted expression body when <paramref name="node"/> is a
        /// map/reduce definition with an expression lambda, and null both when it is not a map node and
        /// when it is a map node whose body cannot be read. Used by the fan-out and unsupported-method
        /// index analyzers.
        /// </summary>
        internal static SyntaxNode? TryGetIndexMapLambdaBody(SyntaxNode node, SemanticModel model, bool includeReduce) =>
            ClassifyIndexMapNode(node, model, includeReduce, out ExpressionSyntax? body) == IndexMapNodeKind.LambdaBody
                ? body
                : null;

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
