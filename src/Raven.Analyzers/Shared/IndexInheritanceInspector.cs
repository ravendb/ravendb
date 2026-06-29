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
        /// Looks for a Map assignment in any constructor of the index class or a user-defined base.
        /// Returns <see cref="IndexChainSearch.Unknown"/> when a base class is only available as
        /// metadata so the caller can suppress rather than report a false positive.
        /// </summary>
        public static IndexChainSearch FindMapAssignmentInChain(INamedTypeSymbol classSymbol, Compilation compilation)
        {
            for (INamedTypeSymbol? type = classSymbol; type != null; type = type.BaseType)
            {
                if (SyntaxHelpers.IsKnownIndexBaseType(type))
                    break; // reached the framework base; it does not assign Map in user source

                if (type.DeclaringSyntaxReferences.IsDefaultOrEmpty)
                    return IndexChainSearch.Unknown; // base compiled in another assembly — can't inspect

                foreach (SyntaxReference syntaxRef in type.DeclaringSyntaxReferences)
                {
                    if (syntaxRef.GetSyntax() is not ClassDeclarationSyntax decl)
                        continue;

                    SemanticModel model = compilation.GetSemanticModel(decl.SyntaxTree);
                    foreach (ConstructorDeclarationSyntax ctor in decl.Members.OfType<ConstructorDeclarationSyntax>())
                    {
                        if (ctor.GetBodyNode() is SyntaxNode body && ContainsMapAssignment(body, model))
                            return IndexChainSearch.Found;
                    }
                }
            }

            return IndexChainSearch.NotFound;
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
