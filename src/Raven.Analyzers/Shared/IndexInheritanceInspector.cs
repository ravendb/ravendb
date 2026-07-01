using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Analyzers.Shared
{
    // Result of searching an index class and its user-defined base classes for a Map/AddMap.
    // Unknown means a base class lives in another (compiled) assembly and cannot be inspected, so
    // the analyzer must not report — the Map/AddMap may well be defined in that base.
    internal enum IndexChainSearch
    {
        Found,
        NotFound,
        Unknown
    }

    /// <summary>
    /// Walks an index class together with its user-defined base classes (up to the framework
    /// Abstract* base) to decide whether Map / AddMap is defined anywhere in the chain. Lives in a
    /// helper class rather than the analyzer so its <see cref="Compilation.GetSemanticModel"/> calls
    /// (needed to inspect base-class constructors that may live in another syntax tree) follow the
    /// same pattern as <c>IndexFieldExtractor</c> and do not trip RS1030.
    /// </summary>
    internal static class IndexInheritanceInspector
    {
        /// <summary>
        /// Looks for a Map assignment reachable from a constructor of the index class or a user-defined
        /// base. Returns <see cref="IndexChainSearch.Unknown"/> when a base class is only available as
        /// metadata so the caller can suppress rather than report a false positive.
        /// </summary>
        public static IndexChainSearch FindMapAssignmentInChain(INamedTypeSymbol classSymbol, Compilation compilation)
        {
            // Collect every in-source class declaration in the chain, stopping at the framework base. A
            // metadata-only base cannot be inspected — the Map may well live there, so report Unknown
            // rather than a false positive.
            if (!TryCollectChainDeclarations(classSymbol, out List<ClassDeclarationSyntax> declarations))
                return IndexChainSearch.Unknown;

            // A Map assignment counts only when it is reachable from a constructor: directly in a ctor
            // body, or in a method the constructor invokes (transitively). Candidate methods and seed
            // constructors are gathered across the WHOLE chain and all partials, so a ctor that delegates
            // to a base-class or other-partial helper — MyIndex() { Setup(); } with void Setup() { Map =
            // ...; } declared elsewhere — is followed correctly. A Map in a method no constructor ever
            // reaches (a dead helper, a finalizer, an operator) does not count, so a genuinely map-less
            // index still reports RVN004. RVN001 separately flags a Map assigned outside a constructor.
            Dictionary<string, List<MethodDeclarationSyntax>> methodsByName = new(StringComparer.Ordinal);
            Queue<(SyntaxNode Body, SemanticModel Model)> pending = new();

            foreach (ClassDeclarationSyntax decl in declarations)
            {
                SemanticModel model = compilation.GetSemanticModel(decl.SyntaxTree);

                foreach (MemberDeclarationSyntax member in decl.Members)
                {
                    if (member is MethodDeclarationSyntax method)
                    {
                        if (!methodsByName.TryGetValue(method.Identifier.Text, out List<MethodDeclarationSyntax>? overloads))
                        {
                            overloads = [];
                            methodsByName[method.Identifier.Text] = overloads;
                        }

                        overloads.Add(method);
                    }
                    else if (member is ConstructorDeclarationSyntax ctor && ctor.GetBodyNode() is SyntaxNode ctorBody)
                    {
                        pending.Enqueue((ctorBody, model));
                    }
                }
            }

            HashSet<string> visitedMethodNames = new(StringComparer.Ordinal);

            while (pending.Count > 0)
            {
                (SyntaxNode body, SemanticModel model) = pending.Dequeue();

                if (ContainsMapAssignment(body, model))
                    return IndexChainSearch.Found;

                // Follow calls to methods declared anywhere in the chain/partials (Setup(), this.Setup(),
                // Setup<T>()); enqueue every overload sharing the name. DescendantNodesAndSelf so an
                // expression-bodied ctor/method (=> Setup()) is covered too. GetMethodName returns null
                // for a bare unqualified call, so fall back to the plain identifier.
                foreach (InvocationExpressionSyntax invocation in body.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
                {
                    string? invokedName = SyntaxHelpers.GetMethodName(invocation);
                    if (invokedName == null && invocation.Expression is IdentifierNameSyntax invokedId)
                        invokedName = invokedId.Identifier.Text;

                    if (invokedName == null || !visitedMethodNames.Add(invokedName))
                        continue;

                    if (methodsByName.TryGetValue(invokedName, out List<MethodDeclarationSyntax>? targets))
                    {
                        foreach (MethodDeclarationSyntax target in targets)
                        {
                            if (target.GetBodyNode() is SyntaxNode targetBody)
                                pending.Enqueue((targetBody, compilation.GetSemanticModel(target.SyntaxTree)));
                        }
                    }
                }
            }

            return IndexChainSearch.NotFound;
        }

        /// <summary>
        /// Collects every in-source class declaration in the chain (each type's partials), stopping at the
        /// framework base. Returns false when a non-framework base is metadata-only (compiled in another
        /// assembly) and therefore cannot be inspected; <paramref name="declarations"/> then holds the
        /// inspectable prefix gathered so far. Shared by the Map search (RVN004) and the field/stored-field
        /// extractors (RVN007/RVN008) so they all treat a base index class the same way and all bail alike
        /// when a base cannot be read.
        /// </summary>
        internal static bool TryCollectChainDeclarations(INamedTypeSymbol classSymbol, out List<ClassDeclarationSyntax> declarations)
        {
            declarations = [];
            for (INamedTypeSymbol? type = classSymbol; type != null; type = type.BaseType)
            {
                if (SyntaxHelpers.IsKnownIndexBaseType(type))
                    break; // reached the framework base; it does not assign Map in user source

                if (type.DeclaringSyntaxReferences.IsDefaultOrEmpty)
                    return false; // base compiled in another assembly — can't inspect

                foreach (SyntaxReference syntaxRef in type.DeclaringSyntaxReferences)
                {
                    if (syntaxRef.GetSyntax() is ClassDeclarationSyntax decl)
                        declarations.Add(decl);
                }
            }

            return true;
        }

        /// <summary>
        /// Counts AddMap/AddMapForAll call sites across the index class and its user-defined base
        /// classes, and reports whether any sit inside a loop and whether a base class is metadata-only
        /// (unknown — the caller must then suppress).
        /// </summary>
        public static (int count, bool anyInLoop, bool unknown) CountAddMapInChain(
            INamedTypeSymbol classSymbol,
            Compilation compilation)
        {
            int count = 0;
            bool anyInLoop = false;

            for (INamedTypeSymbol? type = classSymbol; type != null; type = type.BaseType)
            {
                if (SyntaxHelpers.IsKnownIndexBaseType(type))
                    break;

                if (type.DeclaringSyntaxReferences.IsDefaultOrEmpty)
                    return (count, anyInLoop, true);

                foreach (SyntaxReference syntaxRef in type.DeclaringSyntaxReferences)
                {
                    if (syntaxRef.GetSyntax() is not ClassDeclarationSyntax decl)
                        continue;

                    SemanticModel model = compilation.GetSemanticModel(decl.SyntaxTree);
                    foreach (ConstructorDeclarationSyntax ctor in decl.Members.OfType<ConstructorDeclarationSyntax>())
                    {
                        if (ctor.GetBodyNode() is not SyntaxNode body)
                            continue;

                        foreach (InvocationExpressionSyntax invocation in
                            body.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
                        {
                            string? methodName = SyntaxHelpers.GetMethodName(invocation);
                            if (methodName != KnownTypes.AddMapMethodName && methodName != KnownTypes.AddMapForAllMethodName)
                                continue;

                            // Confirm the method resolves to the AddMap defined on a multi-map index base.
                            ISymbol? symbol = model.GetSymbolInfo(invocation).Symbol;
                            if (symbol is not IMethodSymbol method || !SyntaxHelpers.IsMultiMapBase(method.ContainingType))
                                continue;

                            count++;
                            if (IsInsideLoop(invocation, body))
                                anyInLoop = true;
                        }
                    }
                }
            }

            return (count, anyInLoop, false);
        }

        private static bool ContainsMapAssignment(SyntaxNode node, SemanticModel model)
        {
            foreach (AssignmentExpressionSyntax assignment in
                node.DescendantNodesAndSelf().OfType<AssignmentExpressionSyntax>())
            {
                SimpleNameSyntax? nameNode = SyntaxHelpers.TryGetSimpleMemberName(assignment.Left);
                if (nameNode == null || nameNode.Identifier.Text != KnownTypes.MapFieldName)
                    continue;

                ISymbol? symbol = model.GetSymbolInfo(nameNode).Symbol;
                if (symbol is (IFieldSymbol or IPropertySymbol)
                    && SyntaxHelpers.IsDefinedOnIndexBase(symbol.ContainingType))
                {
                    return true;
                }
            }

            return false;
        }

        // True when <paramref name="node"/> is nested inside a loop construct within <paramref name="body"/>.
        private static bool IsInsideLoop(SyntaxNode node, SyntaxNode body)
        {
            for (SyntaxNode? current = node.Parent; current != null; current = current.Parent)
            {
                if (current is ForStatementSyntax
                            or ForEachStatementSyntax
                            or ForEachVariableStatementSyntax
                            or WhileStatementSyntax
                            or DoStatementSyntax)
                {
                    return true;
                }

                if (current == body)
                    break;
            }

            return false;
        }
    }
}
